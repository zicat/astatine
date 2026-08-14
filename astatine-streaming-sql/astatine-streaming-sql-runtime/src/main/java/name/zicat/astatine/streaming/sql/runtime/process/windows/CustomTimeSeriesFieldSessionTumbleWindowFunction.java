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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.data.RowData.createFieldGetter;

/** CustomTimeSeriesFieldSessionTumbleWindowFunction. */
public class CustomTimeSeriesFieldSessionTumbleWindowFunction extends SessionTumbleWindowFunction {

  private final RowData.FieldGetter timeSeriesGetter;
  private final RowType.RowField timeSeriesField;

  private transient RowData.FieldGetter inputTimeSeriesFieldGetter;

  public CustomTimeSeriesFieldSessionTumbleWindowFunction(
      RowData.FieldGetter eventTimeGetter,
      RowType.RowField eventTimeField,
      RowData.FieldGetter timeSeriesGetter,
      RowType.RowField timeSeriesField,
      int[] fieldMapping,
      RowType.RowField[] fieldTypes,
      int[] valueMapping,
      RowType.RowField[] valueFields,
      RowType.RowField[] originalValueFields,
      String timeSeriesFieldName,
      long sessionMillis,
      long disorderMaxToleranceMillis) {
    super(
        eventTimeGetter,
        eventTimeField,
        fieldMapping,
        fieldTypes,
        valueMapping,
        valueFields,
        originalValueFields,
        timeSeriesFieldName,
        sessionMillis,
        disorderMaxToleranceMillis);
    this.timeSeriesGetter = timeSeriesGetter;
    this.timeSeriesField = timeSeriesField;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.inputTimeSeriesFieldGetter =
        createFieldGetter(timeSeriesField.getType(), fieldTypes.length + valueFields.length + 1);
  }

  @Override
  protected RowData projectInputRowData(RowData rowData, TimestampData eventTime) {
    final var eventTimeRow = new GenericRowData(2);
    eventTimeRow.setField(0, eventTime);
    eventTimeRow.setField(1, timeSeriesGetter.getFieldOrNull(rowData));
    return new MultiJoinedRowData()
        .replace(
            ProjectedRowData.from(fieldMapping).replaceRow(rowData),
            ProjectedRowData.from(valueMapping).replaceRow(rowData),
            eventTimeRow);
  }

  @Override
  protected Object timeSeriesValue(RowData rowData, TimestampData eventTime) {
    return inputTimeSeriesFieldGetter.getFieldOrNull(rowData);
  }

  @Override
  protected BytesAggregationFunction createTimeSeriesHandle() {
    return switch (timeSeriesField.getType().getTypeRoot()) {
      case TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE ->
          new TimeSeries2BytesAggregationFunction();
      case BIGINT -> new Long2BytesAggregationFunction();
      default ->
          throw new IllegalArgumentException(
              "time series field only supports TIMESTAMP or BIGINT: " + timeSeriesField.getType());
    };
  }

  @Override
  protected List<RowType.RowField> inputStateFields() {
    final var inputFields = new ArrayList<>(Arrays.asList(fieldTypes));
    inputFields.addAll(Arrays.asList(valueFields));
    inputFields.add(eventTimeField);
    inputFields.add(timeSeriesField);
    return inputFields;
  }
}
