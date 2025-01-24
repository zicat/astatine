/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package name.zicat.astatine.streaming.sql.runtime.process;

import name.zicat.astatine.streaming.sql.parser.function.KeyedProcessFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataTypeQueryable;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;

import static name.zicat.astatine.streaming.sql.parser.utils.Types.fieldGetter;
import static name.zicat.astatine.streaming.sql.runtime.utils.ProcessUtils.eventTime;
import static name.zicat.astatine.streaming.sql.runtime.utils.ProcessUtils.filterProcessableData;
import static name.zicat.astatine.streaming.sql.runtime.utils.StateUtils.*;
import static org.apache.flink.configuration.ConfigOptions.key;

/** DeduplicateFunctionFactory. */
public class DeduplicateFunctionFactory
    implements KeyedProcessFunctionFactory<RowData, RowData, RowData> {

  public static final String IDENTIFY = "deduplicate";
  public static final ConfigOption<String> OPTION_EVENT_TIME =
      key("eventtime").stringType().noDefaultValue();
  public static final ConfigOption<OrderType> OPTION_ORDER_TYPE =
      key("order.type").enumType(OrderType.class).defaultValue(OrderType.ASC);

  @Override
  public DataStream<RowData> transform(
      TransformContext context, KeyedStream<RowData, RowData> keyedStream) {
    final var type = (DataTypeQueryable) keyedStream.getType();
    final var rowType = (RowType) type.getDataType().getLogicalType();
    final InternalTypeInfo<RowData> rowTypeInfo = InternalTypeInfo.of(rowType);
    final var eventTimeGetter = fieldGetter(rowType, context.get(OPTION_EVENT_TIME));
    final var orderType = context.get(OPTION_ORDER_TYPE);
    final var minRetentionTime =
        context.get(ExecutionConfigOptions.IDLE_STATE_RETENTION).toMillis();
    final var maxRetentionTime = minRetentionTime * 3 / 2;

    final var result =
        keyedStream.process(
            new KeyedProcessFunction<RowData, RowData, RowData>() {

              private transient MapState<Long, RowData> valueState;
              private transient ValueState<Long> registeredTimer;
              private transient ValueState<Long> cleanupTimeState;
              private transient ValueState<RowData> lastRowState;

              @Override
              public void open(Configuration parameters) {
                valueState =
                    getRuntimeContext()
                        .getMapState(new MapStateDescriptor<>("rowState", Types.LONG, rowTypeInfo));
                registeredTimer =
                    getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("registerTime", Types.LONG));
                cleanupTimeState =
                    getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("cleanUpTime", Types.LONG));
                lastRowState =
                    getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("lastRow", rowTypeInfo));
              }

              @Override
              public void processElement(
                  RowData rowData,
                  KeyedProcessFunction<RowData, RowData, RowData>.Context ctx,
                  Collector<RowData> out)
                  throws Exception {
                final var ts = eventTime(eventTimeGetter, rowData);
                valueState.put(ts, rowData);
                registerSmallestTimer(registeredTimer, ts, ctx.timerService());
              }

              @Override
              public void onTimer(
                  long timestamp,
                  KeyedProcessFunction<RowData, RowData, RowData>.OnTimerContext ctx,
                  Collector<RowData> out)
                  throws Exception {
                if (triggerTimeCleanup(timestamp)) {
                  return;
                }
                final var timerService = ctx.timerService();
                final var currentWatermark = timerService.currentWatermark();
                final var processableData = new ArrayList<Map.Entry<Long, RowData>>();
                final var lastUnprocessedTime =
                    filterProcessableData(valueState, currentWatermark, processableData::add);
                if (lastUnprocessedTime < Long.MAX_VALUE) {
                  registerTimer(registeredTimer, lastUnprocessedTime, timerService);
                } else {
                  registeredTimer.clear();
                }
                registerEventCleanupTimer(
                    lastUnprocessedTime == Long.MAX_VALUE ? currentWatermark : lastUnprocessedTime,
                    timerService,
                    cleanupTimeState,
                    minRetentionTime,
                    maxRetentionTime);
                if (processableData.isEmpty()) {
                  return;
                }
                processableData.sort(Map.Entry.comparingByKey());
                final var processEntry =
                    orderType == OrderType.ASC
                        ? processableData.get(0)
                        : processableData.get(processableData.size() - 1);
                final var processRow = processEntry.getValue();
                final var rowInState = lastRowState.value();
                if (rowInState == null) {
                  lastRowState.update(processRow);
                  out.collect(processRow);
                  return;
                }

                final var rowTs = processEntry.getKey();
                final var eventTimeInState = eventTime(eventTimeGetter, rowInState);
                if (orderType == OrderType.ASC && rowTs < eventTimeInState) {
                  lastRowState.update(processRow);
                  out.collect(processRow);
                } else if (orderType == OrderType.DESC && rowTs > eventTimeInState) {
                  lastRowState.update(processRow);
                  out.collect(processRow);
                }
              }

              /**
               * check if trigger processing time cleanup.
               *
               * @return true if triggered
               * @throws Exception Exception
               */
              private boolean triggerTimeCleanup(long timestamp) throws Exception {
                final var cleanupTimestamp = cleanupTimeState.value();
                if (cleanupTimestamp != null && cleanupTimestamp == timestamp) {
                  valueState.clear();
                  registeredTimer.clear();
                  cleanupTimeState.clear();
                  lastRowState.clear();
                  return true;
                }
                return false;
              }
            });
    return result.name(identity() + "_" + result.getId()).returns(keyedStream.getType());
  }

  @Override
  public String identity() {
    return IDENTIFY;
  }

  @Override
  public KeyedProcessFunction<RowData, RowData, RowData> createKeyedProcess(
      TransformContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypeInformation<RowData> returns() {
    throw new UnsupportedOperationException();
  }

  /** OrderType. */
  public enum OrderType implements Serializable {
    ASC,
    DESC
  }
}
