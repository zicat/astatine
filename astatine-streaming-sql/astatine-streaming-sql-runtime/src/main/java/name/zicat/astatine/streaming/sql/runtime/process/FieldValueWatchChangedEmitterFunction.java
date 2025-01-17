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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import static name.zicat.astatine.streaming.sql.runtime.utils.StateUtils.registerEventCleanupTimer;

/**
 * FieldValueWatchChangedEmitterFunction.
 *
 * @param <T>
 */
public class FieldValueWatchChangedEmitterFunction<T>
    extends KeyedProcessFunction<T, RowData, RowData> {

  protected final RowData.FieldGetter fieldGetter;
  protected final int eventTimeIndex;
  protected final RowType.RowField fieldType;
  protected final InternalTypeInfo<RowData> rowTypeInfo;
  protected final long minRetentionTime;
  protected final long maxRetentionTime;

  protected transient ValueState<RowData> previousRowState;
  protected transient MapState<Long, List<RowData>> rowsState;
  protected transient ValueState<Long> registeredTimer;
  protected transient ValueState<Long> cleanupTimeState;

  public FieldValueWatchChangedEmitterFunction(
      RowData.FieldGetter fieldGetter,
      RowType.RowField fieldType,
      int eventTimeIndex,
      long minRetentionTime,
      long maxRetentionTime,
      InternalTypeInfo<RowData> rowTypeInfo) {
    this.fieldGetter = fieldGetter;
    this.fieldType = fieldType;
    this.eventTimeIndex = eventTimeIndex;
    this.minRetentionTime = minRetentionTime;
    this.maxRetentionTime = maxRetentionTime;
    this.rowTypeInfo = rowTypeInfo;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    rowsState =
        getRuntimeContext()
            .getMapState(
                new MapStateDescriptor<>("rowState", Types.LONG, new ListTypeInfo<>(rowTypeInfo)));
    registeredTimer =
        getRuntimeContext().getState(new ValueStateDescriptor<>("registeredTimer", Types.LONG));
    previousRowState =
        getRuntimeContext()
            .getState(
                new ValueStateDescriptor<>(
                    "previousRow",
                    InternalTypeInfo.of(new RowType(Collections.singletonList(fieldType)))));
    cleanupTimeState =
        getRuntimeContext().getState(new ValueStateDescriptor<>("cleanup", Types.LONG));
  }

  @Override
  public void onTimer(
      long timestamp,
      KeyedProcessFunction<T, RowData, RowData>.OnTimerContext ctx,
      Collector<RowData> out)
      throws Exception {
    if (triggerTimeCleanup(timestamp)) {
      return;
    }
    final var timeService = ctx.timerService();
    long lastUnprocessedTime = Long.MAX_VALUE;
    final var leftIt = rowsState.iterator();
    final var currentWatermark = timeService.currentWatermark();
    final var leftData = new ArrayList<Map.Entry<Long, List<RowData>>>();
    while (leftIt.hasNext()) {
      final var entry = leftIt.next();
      final var leftEventTime = entry.getKey();
      if (leftEventTime > currentWatermark) {
        lastUnprocessedTime = Math.min(lastUnprocessedTime, leftEventTime);
        continue;
      }
      leftIt.remove();
      leftData.add(entry);
    }

    leftData.sort(Map.Entry.comparingByKey());
    var previousRow = previousRowState.value();
    try {
      for (var entry : leftData) {
        final var leftStateValueList = entry.getValue();
        for (var leftStateValue : leftStateValueList) {
          final var key = new GenericRowData(1);
          key.setField(0, fieldGetter.getFieldOrNull(leftStateValue));
          if (previousRow == null || !previousRow.equals(key)) {
            previousRow = key;
            out.collect(leftStateValue);
          }
        }
      }
    } finally {
      previousRowState.update(previousRow);
    }

    if (lastUnprocessedTime < Long.MAX_VALUE) {
      registerTimer(lastUnprocessedTime, timeService);
    } else {
      registeredTimer.clear();
    }
    registerEventCleanupTimer(
        lastUnprocessedTime == Long.MAX_VALUE ? currentWatermark : lastUnprocessedTime,
        timeService,
        cleanupTimeState,
        minRetentionTime,
        maxRetentionTime);
  }

  @Override
  public void processElement(
      RowData rowData,
      KeyedProcessFunction<T, RowData, RowData>.Context context,
      Collector<RowData> collector)
      throws Exception {
    final var eventTime = rowData.getTimestamp(eventTimeIndex, 3).getMillisecond();
    if (eventTime <= 0 || eventTime < context.timerService().currentWatermark()) {
      return;
    }
    final var leftStateValueList = rowsState.get(eventTime);
    if (leftStateValueList != null) {
      leftStateValueList.add(rowData);
      return;
    }
    final var rowDataList = new ArrayList<RowData>();
    rowDataList.add(rowData);
    rowsState.put(eventTime, rowDataList);
    final var timeService = context.timerService();
    registerSmallestTimer(eventTime, timeService);
  }

  private void registerSmallestTimer(long timestamp, TimerService timerService) throws IOException {
    Long currentRegisteredTimer = registeredTimer.value();
    if (currentRegisteredTimer == null) {
      registerTimer(timestamp, timerService);
    } else if (currentRegisteredTimer > timestamp) {
      timerService.deleteEventTimeTimer(currentRegisteredTimer);
      registerTimer(timestamp, timerService);
    }
  }

  private void registerTimer(long timestamp, TimerService timerService) throws IOException {
    registeredTimer.update(timestamp);
    timerService.registerEventTimeTimer(timestamp);
  }

  protected void cleanUpdateState() {
    previousRowState.clear();
    rowsState.clear();
    registeredTimer.clear();
    cleanupTimeState.clear();
  }

  /**
   * check if trigger processing time cleanup.
   *
   * @return true if triggered
   * @throws Exception Exception
   */
  protected boolean triggerTimeCleanup(long timestamp) throws Exception {
    final var cleanupTimestamp = cleanupTimeState.value();
    if (cleanupTimestamp != null && cleanupTimestamp == timestamp) {
      cleanUpdateState();
      return true;
    }
    return false;
  }
}
