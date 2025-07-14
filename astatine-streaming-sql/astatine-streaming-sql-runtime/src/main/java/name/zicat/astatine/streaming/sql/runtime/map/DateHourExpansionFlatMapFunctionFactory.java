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

package name.zicat.astatine.streaming.sql.runtime.map;

import name.zicat.astatine.streaming.sql.parser.transform.TransformContext;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;

import static name.zicat.astatine.streaming.sql.parser.utils.Types.unixTimeFieldGetter;

/** DateHourExpansionFlatMapFunctionFactory. */
public class DateHourExpansionFlatMapFunctionFactory extends ExpansionFlatMapFunctionFactoryBase {

  public static final String IDENTITY = "date_hour_expansion";

  @Override
  protected FlatMapFunction<RowData, RowData> create(TransformContext context, RowType rowType) {
    final var timeZoneStr = context.get(OPTION_TIME_ZONE);
    final var startTsFieldGetter = unixTimeFieldGetter(rowType, context.get(OPTION_FIELD_START_TS));
    final var currentTsFieldGetter =
        unixTimeFieldGetter(rowType, context.get(OPTION_FIELD_CURRENT_TS));
    final var endTsFieldGetter = unixTimeFieldGetter(rowType, context.get(OPTION_FIELD_END_TS));
    final var maxPartitionCount = context.get(OPTION_MAX_COUNT);

    return new ExpansionFlatMapFunctionBase(
        timeZoneStr, startTsFieldGetter, currentTsFieldGetter, endTsFieldGetter) {

      private transient GenericRowData rightRow;

      @Override
      public void open(Configuration parameters) {
        super.open(parameters);
        rightRow = new GenericRowData(2);
      }

      @Override
      protected void process(
          RowData rowData,
          LocalDateTime startDate,
          LocalDateTime endDateTime,
          Collector<RowData> collector) {
        final var hoursDifference = (int) calculateHourDifference(startDate, endDateTime);
        final var partitionCount = Math.min(maxPartitionCount, hoursDifference + 1);
        var dateOffset = (int) endDateTime.toLocalDate().toEpochDay();
        var hourOffset = endDateTime.getHour();
        for (var i = 0; i < partitionCount; i++) {
          rightRow.setField(0, dateOffset);
          rightRow.setField(1, hourOffset);
          output(rowData, rightRow, collector);
          if (hourOffset == 0) {
            dateOffset--;
            hourOffset = 23;
          } else {
            hourOffset--;
          }
        }
      }
    };
  }

  @Override
  protected RowType outputType(RowType inputType) {
    final var outputFields = new ArrayList<>(inputType.getFields());
    addField(outputFields, DEFAULT_DATE_FIELD, new DateType(false));
    addField(outputFields, DEFAULT_DATE_HOUR_FIELD, new IntType(false));
    return new RowType(outputFields);
  }

  public static long calculateHourDifference(LocalDateTime start, LocalDateTime end) {
    int startHour = start.getHour();
    int endHour = end.getHour();
    long daysBetween = ChronoUnit.DAYS.between(start.toLocalDate(), end.toLocalDate());
    return daysBetween * 24 + (endHour - startHour);
  }

  @Override
  public String identity() {
    return IDENTITY;
  }
}
