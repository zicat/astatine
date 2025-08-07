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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import java.util.*;

import static name.zicat.astatine.streaming.sql.runtime.utils.RowUtils.toUpdatable;
import static org.apache.flink.table.data.TimestampData.fromEpochMillis;

/** SessionTumble2TumbleWindowFunction. */
public class SessionTumble2TumbleWindowFunction
    extends KeyedProcessFunction<RowData, RowData, RowData> {

  private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";

  private final RowData.FieldGetter eventTimeGetter;
  private final int[] fieldMapping;
  private final int[] valueMapping;
  private final int valueOffset;
  private final int timeSeriesOffset;
  private final List<BytesAggregationFunction> valueHandles = new ArrayList<>();
  private final RowType.RowField[] valueFields;
  private final long tumbleIntervalMs;
  private final RowType returnRowType;
  private final int[] timeSeriesMapping;
  private final int[] fieldMappingInState;
  private final Long2BytesAggregationFunction timeSeriesHandle;
  private final long sessionDurationMs;

  protected transient MapState<Long, RowData> windowState;
  protected transient Counter dropCounter;

  public SessionTumble2TumbleWindowFunction(
      RowData.FieldGetter eventTimeGetter,
      RowType.RowField eventTimeField,
      int[] fieldMapping,
      RowType.RowField[] fieldTypes,
      int[] valueMapping,
      RowType.RowField[] valueFields,
      List<LogicalTypeRoot> valueOriginTypes,
      int timeSeriesMapping,
      RowType.RowField timeSeriesField,
      long tumbleIntervalMs,
      long sessionDurationMs) {
    this.eventTimeGetter = eventTimeGetter;
    this.fieldMapping = fieldMapping;
    this.valueMapping = valueMapping;
    this.valueFields = valueFields;
    this.timeSeriesMapping = new int[] {timeSeriesMapping};
    this.tumbleIntervalMs = tumbleIntervalMs;
    this.sessionDurationMs = sessionDurationMs;
    this.valueOffset = fieldTypes.length + 1; // after event time
    this.fieldMappingInState = new int[fieldTypes.length];
    for (int i = 0; i < fieldMappingInState.length; i++) {
      this.fieldMappingInState[i] = i;
    }
    this.timeSeriesOffset = valueOffset + valueFields.length;
    this.timeSeriesHandle = new Long2BytesAggregationFunction();
    for (var valueRootLogicType : valueOriginTypes) {
      this.valueHandles.add(BytesAggregationFunction.createAggregationFunction(valueRootLogicType));
    }
    this.returnRowType =
        createReturnRowType(eventTimeField, fieldTypes, valueFields, timeSeriesField);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    final var context = getRuntimeContext();
    this.windowState =
        context.getMapState(
            new MapStateDescriptor<>("inputState", Types.LONG, InternalTypeInfo.of(returnRowType)));
    this.dropCounter = context.getMetricGroup().counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
  }

  @Override
  public void processElement(
      RowData rowData,
      KeyedProcessFunction<RowData, RowData, RowData>.Context ctx,
      Collector<RowData> out)
      throws Exception {
    final var eventTimestampData = (TimestampData) eventTimeGetter.getFieldOrNull(rowData);
    if (eventTimestampData == null) {
      dropCounter.inc();
      return;
    }
    final var eventTime = eventTimestampData.getMillisecond();
    final var wk = ctx.timerService().currentWatermark();

    /*
       The wk means the first session value is arrived, all data arrived at (wk - sessionDurationMs),
       currentWindowStart means the start of the current processable tumble window.
    */
    final var currentWindowStart =
        wk == Long.MIN_VALUE ? Long.MIN_VALUE : windowStart(wk - sessionDurationMs);

    /*
       the record may cross multi tumble windows and all ts in session is less than eventTime,
       so if the maxRecordWindowStart is less than currentWindowStart, drop it safely.
    */
    final var maxRecordWindowStart = windowStart(eventTime);
    if (maxRecordWindowStart < currentWindowStart) {
      dropCounter.inc();
      return;
    }

    final var projectedRowData = projectInputRowData(rowData, eventTimestampData);
    final var tsIterator =
        timeSeriesHandle.outputIterator(projectedRowData.getBinary(timeSeriesOffset));
    final var valueIterators = new ArrayList<Iterator<Object>>();
    for (int i = 0; i < valueFields.length; i++) {
      final var valueHandle = valueHandles.get(i);
      valueIterators.add(valueHandle.outputIterator(projectedRowData.getBinary(valueOffset + i)));
    }
    while (tsIterator.hasNext()) {
      final var ts = (long) tsIterator.next();
      final var recordWindowStart = windowStart(ts);
      if (recordWindowStart < currentWindowStart) {
        // the record ts is too old, drop it and skip value.
        for (int i = 0; i < valueFields.length; i++) {
          valueIterators.get(i).next();
        }
        continue;
      }
      // the trigger is the recordWindowEND + sessionDurationMs
      final var recordTrigger = recordWindowStart + tumbleIntervalMs + sessionDurationMs;
      var windowValueState = windowState.get(recordTrigger);
      if (windowValueState == null) {
        final var newValueRow = new GenericRowData(valueFields.length);
        final var timeSeriesRow = new GenericRowData(1);
        final var newEventTimeRow = new GenericRowData(1);
        newEventTimeRow.setField(0, fromEpochMillis(recordWindowStart + tumbleIntervalMs - 1));
        windowValueState =
            new MultiJoinedRowData()
                .replace(
                    ProjectedRowData.from(fieldMappingInState).replaceRow(projectedRowData),
                    newEventTimeRow,
                    newValueRow,
                    timeSeriesRow);
        ctx.timerService().registerEventTimeTimer(recordTrigger);
      }
      final var newValueRow = toUpdatable(windowValueState);
      for (int i = 0; i < valueFields.length; i++) {
        final var valueHandle = valueHandles.get(i);
        final var valueIterator = valueIterators.get(i);
        final var index = valueOffset + i;
        final var accValue = newValueRow.getBinary(index);
        newValueRow.setField(index, valueHandle.accumulate(accValue, valueIterator.next()));
      }
      final var timeSeriesAccValue = newValueRow.getBinary(timeSeriesOffset);
      newValueRow.setField(timeSeriesOffset, timeSeriesHandle.accumulate(timeSeriesAccValue, ts));
      windowState.put(recordTrigger, newValueRow);
    }
  }

  @Override
  public void onTimer(
      long timestamp,
      KeyedProcessFunction<RowData, RowData, RowData>.OnTimerContext ctx,
      Collector<RowData> out)
      throws Exception {
    final var row = windowState.get(timestamp);
    if (row != null) {
      windowState.remove(timestamp);
    }
    if (windowState.isEmpty()) {
      windowState.clear();
    }
    final var updatableRow = toUpdatable(row);
    if (updatableRow == null) {
      return;
    }
    for (int i = 0; i < valueFields.length; i++) {
      final var valueHandle = valueHandles.get(i);
      final var index = valueOffset + i;
      final var accValue = updatableRow.getBinary(index);
      updatableRow.setField(index, valueHandle.output(accValue));
    }
    final var timeSeriesAccValue = updatableRow.getBinary(timeSeriesOffset);
    updatableRow.setField(timeSeriesOffset, timeSeriesHandle.output(timeSeriesAccValue));
    out.collect(updatableRow);
  }

  /**
   * Create the return row type for the session tumble to tumble window function.
   *
   * @param eventTimeField eventTimeField
   * @param fieldTypes fieldTypes
   * @param valueFields valueFields
   * @param timeSeriesField timeSeriesField
   * @return RowType
   */
  private static RowType createReturnRowType(
      RowType.RowField eventTimeField,
      RowType.RowField[] fieldTypes,
      RowType.RowField[] valueFields,
      RowType.RowField timeSeriesField) {
    final var fields = new ArrayList<>(Arrays.asList(fieldTypes));
    fields.add(eventTimeField);
    fields.addAll(Arrays.asList(valueFields));
    fields.add(timeSeriesField);
    return new RowType(fields);
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
            eventTimeRow,
            ProjectedRowData.from(valueMapping).replaceRow(rowData),
            ProjectedRowData.from(timeSeriesMapping).replaceRow(rowData));
  }

  private long windowStart(long eventTime) {
    return (eventTime / tumbleIntervalMs) * tumbleIntervalMs;
  }

  public RowType returnRowType() {
    return returnRowType;
  }
}
