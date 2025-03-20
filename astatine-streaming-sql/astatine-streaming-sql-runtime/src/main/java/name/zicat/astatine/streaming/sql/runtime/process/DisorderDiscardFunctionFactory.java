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
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataTypeQueryable;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static name.zicat.astatine.streaming.sql.parser.utils.Types.fieldGetter;
import static name.zicat.astatine.streaming.sql.runtime.utils.ProcessUtils.addRowDataInListStateAndRegisterTimer;
import static name.zicat.astatine.streaming.sql.runtime.utils.ProcessUtils.filterProcessableData;
import static name.zicat.astatine.streaming.sql.runtime.utils.StateUtils.registerTimer;

/** DisorderDiscardFunctionFactory. */
public class DisorderDiscardFunctionFactory
    implements KeyedProcessFunctionFactory<RowData, RowData, RowData> {

  public static final ConfigOption<String> OPTION_EVENTTIME =
      ConfigOptions.key("eventtime").stringType().noDefaultValue();

  public static final String IDENTITY = "disorder_discard";

  @Override
  public DataStream<RowData> transform(
      TransformContext context, KeyedStream<RowData, RowData> keyedStream) {
    final var type = (DataTypeQueryable) keyedStream.getType();
    final var rowType = (RowType) type.getDataType().getLogicalType();
    final InternalTypeInfo<RowData> rowTypeInfo = InternalTypeInfo.of(rowType);
    final var eventTimeGetter = fieldGetter(rowType, context.get(OPTION_EVENTTIME));
    final var result =
        keyedStream.process(
            new KeyedProcessFunction<RowData, RowData, RowData>() {

              private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME =
                  "numLateRecordsDropped";
              private transient MapState<Long, List<RowData>> valueState;
              private transient ValueState<Long> registeredTimer;
              private transient Counter dropCounter;

              @Override
              public void open(Configuration parameters) {
                final var context = getRuntimeContext();
                valueState =
                    context.getMapState(
                        new MapStateDescriptor<>(
                            "rowState", Types.LONG, new ListTypeInfo<>(rowTypeInfo)));
                registeredTimer =
                    context.getState(new ValueStateDescriptor<>("registerTime", Types.LONG));
                dropCounter = context.getMetricGroup().counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
              }

              @Override
              public void processElement(
                  RowData rowData,
                  KeyedProcessFunction<RowData, RowData, RowData>.Context context,
                  Collector<RowData> collector)
                  throws Exception {
                if (!addRowDataInListStateAndRegisterTimer(
                    eventTimeGetter,
                    rowData,
                    valueState,
                    registeredTimer,
                    context.timerService(),
                    true)) {
                  dropCounter.inc();
                }
              }

              @Override
              public void onTimer(
                  long timestamp,
                  KeyedProcessFunction<RowData, RowData, RowData>.OnTimerContext ctx,
                  Collector<RowData> out)
                  throws Exception {

                final var timerService = ctx.timerService();
                final var currentWatermark = timerService.currentWatermark();
                final var processableData = new ArrayList<Map.Entry<Long, List<RowData>>>();
                final var lastUnprocessedTime =
                    filterProcessableData(valueState, currentWatermark, processableData::add);
                processableData.sort(Map.Entry.comparingByKey());
                for (var entry : processableData) {
                  for (var row : entry.getValue()) {
                    out.collect(row);
                  }
                }
                if (lastUnprocessedTime < Long.MAX_VALUE) {
                  registerTimer(registeredTimer, lastUnprocessedTime, timerService);
                } else {
                  registeredTimer.clear();
                  valueState.clear();
                }
              }
            });
    return result.name(identity() + "_" + result.getId()).returns(keyedStream.getType());
  }

  @Override
  public String identity() {
    return IDENTITY;
  }

  @Override
  public TypeInformation<RowData> returns() {
    throw new UnsupportedOperationException();
  }

  @Override
  public KeyedProcessFunction<RowData, RowData, RowData> createKeyedProcess(
      TransformContext context) {
    throw new UnsupportedOperationException();
  }
}
