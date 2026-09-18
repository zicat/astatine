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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.heap.AbstractHeapState;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;

import static name.zicat.astatine.streaming.sql.runtime.utils.ProcessUtils.eventTime;
import static name.zicat.astatine.streaming.sql.runtime.utils.ProcessUtils.filterProcessableData;
import static name.zicat.astatine.streaming.sql.runtime.utils.StateUtils.registerEventCleanupTimer;
import static name.zicat.astatine.streaming.sql.runtime.utils.StateUtils.registerSmallestTimer;
import static name.zicat.astatine.streaming.sql.runtime.utils.StateUtils.registerTimer;

/** DeduplicateFunction. */
public class DeduplicateFunction extends KeyedProcessFunction<RowData, RowData, RowData> {

  /** OrderType. */
  public enum OrderType implements Serializable {
    ASC,
    DESC
  }

  protected final RowData.FieldGetter eventTimeGetter;
  protected final InternalTypeInfo<RowData> rowTypeInfo;
  protected final OrderType orderType;
  protected final long minRetentionTime;
  protected final long maxRetentionTime;

  protected transient MapState<Long, RowData> valueState;
  protected transient ValueState<Long> registeredTimer;
  protected transient ValueState<Long> cleanupTimeState;
  protected transient ValueState<RowData> lastRowState;
  protected transient TypeSerializer<RowData> inputStateRowSerializer;

  public DeduplicateFunction(
      RowData.FieldGetter eventTimeGetter,
      InternalTypeInfo<RowData> rowTypeInfo,
      OrderType orderType,
      long minRetentionTime,
      long maxRetentionTime) {
    this.eventTimeGetter = eventTimeGetter;
    this.rowTypeInfo = rowTypeInfo;
    this.orderType = orderType;
    this.minRetentionTime = minRetentionTime;
    this.maxRetentionTime = maxRetentionTime;
  }

  @SuppressWarnings("deprecation")
  @Override
  public void open(Configuration parameters) {
    valueState =
        getRuntimeContext()
            .getMapState(new MapStateDescriptor<>("rowState", Types.LONG, rowTypeInfo));
    registeredTimer =
        getRuntimeContext().getState(new ValueStateDescriptor<>("registerTime", Types.LONG));
    cleanupTimeState =
        getRuntimeContext().getState(new ValueStateDescriptor<>("cleanUpTime", Types.LONG));
    lastRowState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastRow", rowTypeInfo));
    final var objectReuseEnabled = getRuntimeContext().isObjectReuseEnabled();
    if ((registeredTimer instanceof AbstractHeapState) && objectReuseEnabled) {
      inputStateRowSerializer =
          rowTypeInfo.createSerializer(
              getRuntimeContext().getExecutionConfig().getSerializerConfig());
    }
  }

  @Override
  public void processElement(
      RowData rowData,
      KeyedProcessFunction<RowData, RowData, RowData>.Context ctx,
      Collector<RowData> out)
      throws Exception {
    final var ts = eventTime(eventTimeGetter, rowData);
    if (inputStateRowSerializer != null) {
      rowData = inputStateRowSerializer.copy(rowData);
    }
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

  private boolean triggerTimeCleanup(long timestamp) throws Exception {
    final var cleanupTimestamp = cleanupTimeState.value();
    if (cleanupTimestamp != null && cleanupTimestamp == timestamp) {
      lastRowState.clear();
      cleanupTimeState.clear();
      if (registeredTimer.value() == null) {
        valueState.clear();
        registeredTimer.clear();
        return true;
      }
    }
    return false;
  }
}
