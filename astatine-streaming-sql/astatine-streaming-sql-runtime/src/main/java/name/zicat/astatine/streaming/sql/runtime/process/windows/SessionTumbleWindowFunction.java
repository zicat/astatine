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

package name.zicat.astatine.streaming.sql.runtime.process.windows;

import name.zicat.astatine.streaming.sql.runtime.utils.MultiJoinedRowData;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.UpdatableRowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.*;

import static org.apache.flink.table.data.RowData.createFieldGetter;
import static org.apache.flink.table.data.TimestampData.fromEpochMillis;

/** SessionTumbleWindowFunction. */
public class SessionTumbleWindowFunction extends KeyedProcessFunction<RowData, RowData, RowData> {

  private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";

  protected final RowData.FieldGetter eventTimeGetter;
  protected final RowType.RowField eventTimeField;
  protected final int[] fieldMapping;
  protected final RowType.RowField[] fieldTypes;
  protected final int[] valueMapping;
  protected final RowType.RowField[] valueFields;
  protected final RowType valueStateType;
  protected final RowData.FieldGetter[] inputValueFieldGetters;
  protected final List<AggregationFunction<byte[]>> valueHandles;
  protected final TimeSeriesAggregationFunction timeSeriesHandle;
  protected final int[] fieldInProjectRow;
  protected final long sessionMillis;
  protected final int timeSeriesOffset;
  protected final int eventTimeOffset;
  protected final int fieldsOffset;
  protected final int valuesOffset;

  protected transient ValueState<RowData> valueState;
  protected transient ValueState<Long> registeredTimer;
  protected transient MapState<Long, List<RowData>> inputState;
  protected transient ProcessableRows processableRows;
  protected transient Counter dropCounter;

  public SessionTumbleWindowFunction(
      RowData.FieldGetter eventTimeGetter,
      RowType.RowField eventTimeField,
      int[] fieldMapping,
      RowType.RowField[] fieldTypes,
      int[] valueMapping,
      RowType.RowField[] valueFields,
      String timeSeriesFieldName,
      long sessionMillis) {
    this.eventTimeGetter = eventTimeGetter;
    this.eventTimeField = eventTimeField;
    this.fieldMapping = fieldMapping;
    this.fieldTypes = fieldTypes;
    this.valueMapping = valueMapping;
    this.valueFields = valueFields;
    this.sessionMillis = sessionMillis;
    this.fieldsOffset = 0;
    this.valuesOffset = fieldTypes.length;
    this.eventTimeOffset = valuesOffset + valueFields.length;
    this.timeSeriesOffset = eventTimeOffset + 1;
    this.fieldInProjectRow = new int[fieldTypes.length];
    for (int i = 0; i < fieldTypes.length; i++) {
      this.fieldInProjectRow[i] = i;
    }
    this.inputValueFieldGetters = new RowData.FieldGetter[valueFields.length];
    this.valueHandles = new ArrayList<>(valueFields.length);
    final var valueOffset = fieldTypes.length;
    for (int i = 0; i < valueFields.length; i++) {
      this.inputValueFieldGetters[i] = createFieldGetter(valueFields[i].getType(), valueOffset + i);
      this.valueHandles.add(
          BytesAggregationFunction.createAggregationFunction(valueFields[i].getType()));
    }
    this.timeSeriesHandle = new TimeSeriesAggregationFunction();
    this.valueStateType =
        createReturnRowType(eventTimeField, fieldTypes, valueFields, timeSeriesFieldName);
  }

  @Override
  public void open(Configuration parameters) throws Exception {

    final var context = getRuntimeContext();
    this.valueState =
        context.getState(
            new ValueStateDescriptor<>("valueState", InternalTypeInfo.of(valueStateType)));
    this.registeredTimer = context.getState(new ValueStateDescriptor<>("timer", Types.LONG));
    final var inputFields = new ArrayList<>(Arrays.asList(fieldTypes));
    inputFields.addAll(Arrays.asList(valueFields));
    inputFields.add(eventTimeField);
    inputState =
        context.getMapState(
            new MapStateDescriptor<>(
                "inputState",
                Types.LONG,
                new ListTypeInfo<>(InternalTypeInfo.of(new RowType(inputFields)))));
    this.dropCounter = context.getMetricGroup().counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
    this.processableRows = new ProcessableRows();
  }

  @Override
  public void processElement(
      RowData rowData,
      KeyedProcessFunction<RowData, RowData, RowData>.Context context,
      Collector<RowData> collector)
      throws Exception {
    final var eventTimestampData = (TimestampData) eventTimeGetter.getFieldOrNull(rowData);
    if (eventTimestampData == null) {
      return;
    }
    final var timeService = context.timerService();
    final var currentWatermark = timeService.currentWatermark();
    final long eventTime = eventTimestampData.getMillisecond();
    if (eventTime < currentWatermark) {
      dropCounter.inc();
      return;
    }
    rowData = projectInputRowData(rowData, eventTimestampData);
    var rowInState = valueState.value();
    if (rowInState == null) {
      putAndRegisterSmallestTimer(rowData, eventTime, timeService);
      return;
    }
    final var delta = eventTime - getStartTime(rowInState);
    if (delta >= sessionMillis) {
      putAndRegisterSmallestTimer(rowData, eventTime, timeService);
      return;
    }
    // in-order accumulate values and time series directly
    valueState.update(
        accumulateValuesTimeSeries(toUpdatable(rowInState), rowData, eventTimestampData));
  }

  @Override
  public void onTimer(
      long timestamp,
      KeyedProcessFunction<RowData, RowData, RowData>.OnTimerContext ctx,
      Collector<RowData> out)
      throws Exception {

    UpdatableRowData stateRow = toUpdatable(valueState.value());
    if (stateRow != null && getStartTime(stateRow) + sessionMillis == timestamp) {
      stateRow =
          collectIfNeed(stateRow, timestamp, out)
              ? initStateRowData(stateRow, timestamp, timestamp)
              : null;
    }

    final var timerService = ctx.timerService();
    final var processableRows = processableSortedRows(timerService.currentWatermark());
    final var processableInputRows = processableRows.processableRows;

    var windowEnd = stateRow != null ? getStartTime(stateRow) + sessionMillis : null;
    for (var entry : processableInputRows) {
      final var eventTime = entry.getKey();
      for (var row : entry.getValue()) {
        if (windowEnd == null) {
          stateRow = initStateRowData(row, eventTime, eventTime);
          windowEnd = eventTime + sessionMillis;
        } else if (windowEnd < eventTime) {
          final var notEmpty = collectIfNeed(stateRow, windowEnd, out);
          if (notEmpty && eventTime < windowEnd + sessionMillis) {
            stateRow = initStateRowData(row, eventTime, windowEnd);
            windowEnd = windowEnd + sessionMillis;
          } else {
            stateRow = initStateRowData(row, eventTime, eventTime);
            windowEnd = eventTime + sessionMillis;
          }
        }
        stateRow = accumulateValuesTimeSeries(stateRow, row, fromEpochMillis(eventTime));
      }
    }

    final var lastUnprocessedTime = processableRows.lastUnprocessedTime;
    if (windowEnd == null && lastUnprocessedTime == Long.MAX_VALUE) {
      cleanUpdateState();
      return;
    }
    final var nextEventTime =
        windowEnd == null ? lastUnprocessedTime : Math.min(windowEnd, lastUnprocessedTime);
    registerTimer(nextEventTime, timerService);
    valueState.update(stateRow);
  }

  private ProcessableRows processableSortedRows(long watermark) throws Exception {
    final var processableInputRows = new ArrayList<Map.Entry<Long, List<RowData>>>();
    final var it = inputState.entries().iterator();
    var lastUnprocessedTime = Long.MAX_VALUE;
    while (it.hasNext()) {
      final var entry = it.next();
      final var eventTime = entry.getKey();
      if (eventTime > watermark) {
        lastUnprocessedTime = Math.min(lastUnprocessedTime, eventTime);
        continue;
      }
      it.remove();
      processableInputRows.add(entry);
    }
    processableInputRows.sort(Map.Entry.comparingByKey());
    return processableRows.replace(lastUnprocessedTime, processableInputRows);
  }

  protected boolean collectIfNeed(
      UpdatableRowData updatableRow, long timestamp, Collector<RowData> out) {
    if (emptyElementInStateRow(updatableRow)) {
      return false;
    }
    updatableRow.setTimestamp(eventTimeOffset, fromEpochMillis(timestamp), 3);
    getValuesTimeSeries(updatableRow);
    out.collect(updatableRow);
    return true;
  }

  protected UpdatableRowData initStateRowData(RowData rowData, long eventTime, long startTime) {
    final var timestampRow = new GenericRowData(2);
    timestampRow.setField(0, fromEpochMillis(eventTime));
    timestampRow.setField(1, timeSeriesHandle.accumulate(null, startTime));
    final var valueRow = new GenericRowData(valueFields.length);
    if (rowData instanceof UpdatableRowData updatableRowData
        && updatableRowData.getRow() instanceof MultiJoinedRowData multiJoinedRowData) {
      multiJoinedRowData.replace(1, valueRow);
      multiJoinedRowData.replace(2, timestampRow);
      return new UpdatableRowData(multiJoinedRowData, multiJoinedRowData.getArity());
    }
    final var joinedRowData = new MultiJoinedRowData();
    joinedRowData.replace(
        ProjectedRowData.from(fieldInProjectRow).replaceRow(rowData), valueRow, timestampRow);
    return new UpdatableRowData(joinedRowData, joinedRowData.getArity());
  }

  private static UpdatableRowData toUpdatable(RowData rowData) {
    if (rowData == null) {
      return null;
    }
    return rowData instanceof UpdatableRowData
        ? (UpdatableRowData) rowData
        : new UpdatableRowData(rowData, rowData.getArity());
  }

  protected void cleanUpdateState() {
    registeredTimer.clear();
    valueState.clear();
    inputState.clear();
  }

  protected UpdatableRowData accumulateValuesTimeSeries(
      UpdatableRowData rowInState, RowData rowData, TimestampData eventTime) {
    if (emptyElementInStateRow(rowInState)) {
      // reset keys by current rowData if value is empty
      final var preEventTime = rowInState.getTimestamp(eventTimeOffset, 3);
      final var startTime = getStartTime(rowInState);
      rowInState = initStateRowData(rowData, preEventTime.getMillisecond(), startTime);
    }
    final var eventTimeMs = eventTime.getMillisecond();
    rowInState.setTimestamp(eventTimeOffset, eventTime, 3);
    for (int i = 0; i < valueFields.length; i++) {
      final var value = inputValueFieldGetters[i].getFieldOrNull(rowData);
      final var valueOffset = i + valuesOffset;
      final var valueInstate = rowInState.getBinary(valueOffset);
      rowInState.setField(valueOffset, valueHandles.get(i).accumulate(valueInstate, value));
    }
    rowInState.setField(
        timeSeriesOffset,
        timeSeriesHandle.accumulate(rowInState.getBinary(timeSeriesOffset), eventTimeMs));
    return rowInState;
  }

  private void getValuesTimeSeries(UpdatableRowData rowInState) {
    for (int i = 0; i < valueFields.length; i++) {
      final var valueOffset = i + valuesOffset;
      final var value = rowInState.getBinary(valueOffset);
      rowInState.setField(valueOffset, valueHandles.get(i).output(value));
    }
    rowInState.setField(
        timeSeriesOffset, timeSeriesHandle.output(rowInState.getBinary(timeSeriesOffset)));
  }

  private void putAndRegisterSmallestTimer(
      RowData rowData, long eventTime, TimerService timeService) throws Exception {
    var listRows = inputState.get(eventTime);
    if (listRows == null) {
      listRows = new ArrayList<>();
      inputState.put(eventTime, listRows);
      registerSmallestTimer(eventTime, timeService);
    }
    listRows.add(rowData);
  }

  /**
   * project input row data. the order is fields, values, event time.
   *
   * @param rowData rowData
   * @param eventTime eventTime
   * @return RowData
   */
  private RowData projectInputRowData(RowData rowData, TimestampData eventTime) {
    var eventTimeRow = new GenericRowData(1);
    eventTimeRow.setField(0, eventTime);
    return new MultiJoinedRowData()
        .replace(
            ProjectedRowData.from(fieldMapping).replaceRow(rowData),
            ProjectedRowData.from(valueMapping).replaceRow(rowData),
            eventTimeRow);
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

  public RowType returnRowType() {
    return valueStateType;
  }

  private static RowType createReturnRowType(
      RowType.RowField eventTimeField,
      RowType.RowField[] fieldTypes,
      RowType.RowField[] valueFields,
      String timeSeriesFieldName) {
    final var fields = new ArrayList<>(Arrays.asList(fieldTypes));
    for (final RowType.RowField valueField : valueFields) {
      fields.add(new RowType.RowField(valueField.getName(), new VarBinaryType()));
    }
    fields.add(eventTimeField);
    fields.add(new RowType.RowField(timeSeriesFieldName, new VarBinaryType()));
    return new RowType(fields);
  }

  private Long getStartTime(RowData rowInState) {
    return timeSeriesHandle.firstValue(rowInState.getBinary(timeSeriesOffset));
  }

  public boolean emptyElementInStateRow(RowData rowInState) {
    return timeSeriesHandle.valueSize(rowInState.getBinary(timeSeriesOffset)) == 0;
  }

  /** ProcessableRow. */
  protected static class ProcessableRows {
    public Long lastUnprocessedTime = Long.MAX_VALUE;
    public List<Map.Entry<Long, List<RowData>>> processableRows = Collections.emptyList();

    public ProcessableRows replace(
        Long lastUnprocessedTime, List<Map.Entry<Long, List<RowData>>> processableRows) {
      this.lastUnprocessedTime = lastUnprocessedTime;
      this.processableRows = processableRows;
      return this;
    }
  }
}
