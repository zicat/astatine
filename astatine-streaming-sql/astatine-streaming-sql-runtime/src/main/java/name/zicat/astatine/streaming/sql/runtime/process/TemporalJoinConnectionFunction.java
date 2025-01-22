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

import static name.zicat.astatine.streaming.sql.runtime.utils.StateUtils.registerEventCleanupTimer;

import name.zicat.astatine.streaming.sql.runtime.utils.UpdatableProjectRowData;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.util.*;

/** TemporalJoinConnectionFunction. */
public class TemporalJoinConnectionFunction<T>
    extends KeyedCoProcessFunction<T, RowData, RowData, RowData> {

  private static final String LEFT_STATE_NAME = "left";
  private static final String RIGHT_STATE_NAME = "right";
  private static final String CLEANUP_STATE_NAME = "cleanUp";
  private static final String REGISTERED_TIMER_STATE_NAME = "smallTimer";
  protected final int leftEventTimeIndex;
  protected final int rightEventTimeIndex;
  protected final long minRetentionTime;
  protected final long maxRetentionTime;
  protected final RowtimeComparator rightRowtimeComparator;
  protected final TemporalJoinConnectionFunctionFactory.JoinType joinType;
  protected final InternalTypeInfo<RowData> leftReturnRowTypeInfo;
  protected final InternalTypeInfo<RowData> rightReturnRowTypeInfo;
  protected final int[] leftReturnIndexMapping;
  protected final int[] rightReturnIndexMapping;

  protected transient MapState<Long, List<RowData>> leftState;
  protected transient MapState<Long, RowData> rightState;
  protected transient ValueState<Long> cleanupTimeState;
  protected transient ValueState<Long> registeredTimer;
  protected transient RowData rightNullRow;
  protected transient JoinedRowData returnRowData;

  public TemporalJoinConnectionFunction(
      InternalTypeInfo<RowData> leftReturnRowTypeInfo,
      InternalTypeInfo<RowData> rightReturnRowTypeInfo,
      int leftEventTimeIndex,
      int rightEventTimeIndex,
      long minRetentionTime,
      long maxRetentionTime,
      TemporalJoinConnectionFunctionFactory.JoinType joinType,
      int[] leftReturnIndexMapping,
      int[] rightReturnIndexMapping) {
    this.leftReturnRowTypeInfo = leftReturnRowTypeInfo;
    this.rightReturnRowTypeInfo = rightReturnRowTypeInfo;
    this.leftEventTimeIndex = leftEventTimeIndex;
    this.rightEventTimeIndex = rightEventTimeIndex;
    this.minRetentionTime = minRetentionTime;
    this.maxRetentionTime = maxRetentionTime;
    this.rightRowtimeComparator = new RowtimeComparator();
    this.joinType = joinType;
    this.leftReturnIndexMapping = leftReturnIndexMapping;
    this.rightReturnIndexMapping = rightReturnIndexMapping;
  }

  @Override
  public void open(Configuration parameters) {
    leftState =
        getRuntimeContext()
            .getMapState(
                new MapStateDescriptor<>(
                    LEFT_STATE_NAME, Types.LONG, new ListTypeInfo<>(leftReturnRowTypeInfo)));
    rightState =
        getRuntimeContext()
            .getMapState(
                new MapStateDescriptor<>(RIGHT_STATE_NAME, Types.LONG, rightReturnRowTypeInfo));
    cleanupTimeState =
        getRuntimeContext().getState(new ValueStateDescriptor<>(CLEANUP_STATE_NAME, Types.LONG));
    registeredTimer =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>(REGISTERED_TIMER_STATE_NAME, Types.LONG));
    rightNullRow = new GenericRowData(rightReturnIndexMapping.length);
    returnRowData = new JoinedRowData();
  }

  @Override
  public void processElement1(
      RowData row,
      KeyedCoProcessFunction<T, RowData, RowData, RowData>.Context context,
      Collector<RowData> collector)
      throws Exception {
    final var eventTime = row.getTimestamp(leftEventTimeIndex, 3).getMillisecond();
    if (eventTime <= 0 || eventTime == Long.MAX_VALUE) {
      return;
    }
    final var rowData = projectRow(row, leftReturnIndexMapping);
    final var leftStateValueList = leftState.get(eventTime);
    if (leftStateValueList != null) {
      leftStateValueList.add(rowData);
      return;
    }
    final var rowDataList = new ArrayList<RowData>();
    rowDataList.add(rowData);
    leftState.put(eventTime, rowDataList);
    final var timeService = context.timerService();
    registerSmallestTimer(eventTime, timeService);
  }

  @Override
  public void processElement2(
      RowData row,
      KeyedCoProcessFunction<T, RowData, RowData, RowData>.Context context,
      Collector<RowData> collector)
      throws Exception {
    final var eventTime = row.getTimestamp(rightEventTimeIndex, 3).getMillisecond();
    if (eventTime <= 0 || eventTime == Long.MAX_VALUE) {
      return;
    }
    final var rowData = projectRow(row, rightReturnIndexMapping);
    rightState.put(eventTime, rowData);
    final var timeService = context.timerService();
    registerSmallestTimer(eventTime, timeService);
  }

  @Override
  public void onTimer(
      long timestamp,
      KeyedCoProcessFunction<T, RowData, RowData, RowData>.OnTimerContext ctx,
      Collector<RowData> out)
      throws Exception {

    if (triggerTimeCleanup(ctx, timestamp)) {
      return;
    }

    final var timeService = ctx.timerService();
    long lastUnprocessedTime = Long.MAX_VALUE;
    final var leftIt = leftState.iterator();
    final var currentWatermark = timeService.currentWatermark();
    final var rightRowsSorted = getRightRowSorted(rightRowtimeComparator);
    while (leftIt.hasNext()) {
      final var entry = leftIt.next();
      final var leftEventTime = entry.getKey();
      if (leftEventTime > currentWatermark) {
        lastUnprocessedTime = Math.min(lastUnprocessedTime, leftEventTime);
        continue;
      }
      leftIt.remove();
      final var leftSideRows = entry.getValue();
      for (var leftSideRow : leftSideRows) {
        emitRow(leftSideRow, leftEventTime, rightRowsSorted, out);
      }
    }
    cleanupExpiredVersionInState(currentWatermark, rightRowsSorted);
    // left and right row is empty, clear state
    if (lastUnprocessedTime == Long.MAX_VALUE && rightState.isEmpty()) {
      cleanUpdateState(ctx);
      return;
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

  protected void emitRow(
      RowData leftSideRow,
      long leftEventTime,
      List<Map.Entry<Long, RowData>> rightRowsSorted,
      Collector<RowData> out)
      throws IOException {
    var rightRowOption = latestRightRowToJoin(rightRowsSorted, leftEventTime);
    if (joinType == TemporalJoinConnectionFunctionFactory.JoinType.LEFT) {
      collectJoinedRow(
          leftSideRow,
          rightRowOption.isPresent() ? rightRowOption.get().getValue() : rightNullRow,
          out);
    } else if (rightRowOption.isPresent()) {
      collectJoinedRow(leftSideRow, rightRowOption.get().getValue(), out);
    }
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

  /**
   * check if trigger processing time cleanup.
   *
   * @param ctx ctx
   * @return true if triggered
   * @throws Exception Exception
   */
  protected boolean triggerTimeCleanup(
      KeyedCoProcessFunction<T, RowData, RowData, RowData>.OnTimerContext ctx, long timestamp)
      throws Exception {
    final var cleanupTimestamp = cleanupTimeState.value();
    if (cleanupTimestamp != null && cleanupTimestamp == timestamp) {
      cleanUpdateState(ctx);
      return true;
    }
    return false;
  }

  protected void cleanUpdateState(
      KeyedCoProcessFunction<T, RowData, RowData, RowData>.OnTimerContext ctx) throws IOException {
    registeredTimer.clear();
    leftState.clear();
    rightState.clear();
    cleanupTimeState.clear();
  }

  protected void collectJoinedRow(RowData leftSideRow, RowData rightRow, Collector<RowData> out)
      throws IOException {
    returnRowData.setRowKind(leftSideRow.getRowKind());
    returnRowData.replace(leftSideRow, rightRow);
    out.collect(returnRowData);
  }

  /** Removes all expired version in the versioned table's state according to current watermark. */
  private void cleanupExpiredVersionInState(
      long currentWatermark, List<Map.Entry<Long, RowData>> rightRowsSorted) throws Exception {
    int i = 0;
    int indexToKeep = firstIndexToKeep(currentWatermark, rightRowsSorted);
    // clean old version data that behind current watermark
    while (i < indexToKeep) {
      rightState.remove(rightRowsSorted.get(i).getKey());
      i += 1;
    }
  }

  private int firstIndexToKeep(
      long timerTimestamp, List<Map.Entry<Long, RowData>> rightRowsSorted) {
    int firstIndexNewerThenTimer =
        indexOfFirstElementNewerThanTimer(timerTimestamp, rightRowsSorted);
    if (firstIndexNewerThenTimer < 0) {
      return rightRowsSorted.size() - 1;
    } else {
      return firstIndexNewerThenTimer - 1;
    }
  }

  private int indexOfFirstElementNewerThanTimer(
      long timerTimestamp, List<Map.Entry<Long, RowData>> list) {
    final var it = list.listIterator();
    while (it.hasNext()) {
      if (it.next().getKey() > timerTimestamp) {
        return it.previousIndex();
      }
    }
    return -1;
  }

  private List<Map.Entry<Long, RowData>> getRightRowSorted(RowtimeComparator rowtimeComparator)
      throws Exception {
    List<Map.Entry<Long, RowData>> rightRows = new ArrayList<>();
    for (var entry : rightState.entries()) {
      rightRows.add(entry);
    }
    rightRows.sort(rowtimeComparator);
    return rightRows;
  }

  protected Optional<Map.Entry<Long, RowData>> latestRightRowToJoin(
      List<Map.Entry<Long, RowData>> rightRowsSorted, long leftTime) {
    return latestRightRowToJoin(rightRowsSorted, 0, rightRowsSorted.size() - 1, leftTime);
  }

  private Optional<Map.Entry<Long, RowData>> latestRightRowToJoin(
      List<Map.Entry<Long, RowData>> rightRowsSorted, int low, int high, long leftTime) {
    if (low > high) {
      if (low - 1 < 0) {
        return Optional.empty();
      } else {
        return Optional.of(rightRowsSorted.get(low - 1));
      }
    } else {
      final var mid = (low + high) >>> 1;
      final var entry = rightRowsSorted.get(mid);
      final var midTime = entry.getKey();
      final var cmp = Long.compare(midTime, leftTime);
      if (cmp < 0) {
        return latestRightRowToJoin(rightRowsSorted, mid + 1, high, leftTime);
      } else if (cmp > 0) {
        return latestRightRowToJoin(rightRowsSorted, low, mid - 1, leftTime);
      } else {
        return Optional.of(entry);
      }
    }
  }

  /** RowtimeComparator. */
  public static class RowtimeComparator implements Comparator<Map.Entry<Long, ?>>, Serializable {

    @Serial private static final long serialVersionUID = 8160134014590716914L;

    private RowtimeComparator() {}

    @Override
    public int compare(Map.Entry<Long, ?> o1, Map.Entry<Long, ?> o2) {
      long o1Time = o1.getKey();
      long o2Time = o2.getKey();
      return Long.compare(o1Time, o2Time);
    }
  }

  private static UpdatableProjectRowData projectRow(RowData row, int[] indexMapping) {
    return UpdatableProjectRowData.from(indexMapping).replaceRow(row);
  }
}
