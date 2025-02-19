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

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.TimeZone;

/** ExpansionFlatMapFunctionBase. */
public abstract class ExpansionFlatMapFunctionBase extends RichFlatMapFunction<RowData, RowData> {

  private final String timeZoneStr;
  private final RowData.FieldGetter startTsFieldGetter;
  private final RowData.FieldGetter currentTsFieldGetter;
  private final RowData.FieldGetter endTsFieldGetter;

  protected transient JoinedRowData returnRowData;
  protected transient ZoneId zoneId;

  public ExpansionFlatMapFunctionBase(
      String timeZoneStr,
      RowData.FieldGetter startTsFieldGetter,
      RowData.FieldGetter currentTsFieldGetter,
      RowData.FieldGetter endTsFieldGetter) {
    this.timeZoneStr = timeZoneStr;
    this.startTsFieldGetter = startTsFieldGetter;
    this.currentTsFieldGetter = currentTsFieldGetter;
    this.endTsFieldGetter = endTsFieldGetter;
  }

  @Override
  public void open(Configuration parameters) {
    zoneId = TimeZone.getTimeZone(timeZoneStr).toZoneId();
    returnRowData = new JoinedRowData();
  }

  @Override
  public void flatMap(RowData rowData, Collector<RowData> collector) {
    final var startTs = getUnixTimestamp(startTsFieldGetter, rowData);
    final var currentTs = getUnixTimestamp(currentTsFieldGetter, rowData);
    final var endTs = getUnixTimestamp(endTsFieldGetter, rowData);
    if (currentTs == null || currentTs == 0) {
      throw new NullPointerException("currentTs is null");
    }
    final var endDateTime = legalTs(endTs) ? localDateTime(endTs) : localDateTime(currentTs);
    final var startDateTime =
        legalTs(startTs) ? localDateTime(startTs) : LocalDateTime.ofInstant(Instant.EPOCH, zoneId);

    if (endDateTime.isBefore(startDateTime)) {
      throw new IllegalStateException(
          "endDateTime < startDateTime, endDateTime: "
              + endDateTime
              + ", startDateTime: "
              + startDateTime);
    }
    process(rowData, startDateTime, endDateTime, collector);
  }

  protected abstract void process(
      RowData rowData,
      LocalDateTime startDate,
      LocalDateTime endDateTime,
      Collector<RowData> collector);

  protected Integer getUnixTimestamp(RowData.FieldGetter tsGetter, RowData rowData) {
    final var value = tsGetter.getFieldOrNull(rowData);
    if (value == null) {
      return null;
    } else if (value instanceof Long longValue) {
      return (int) (longValue / 1000);
    } else if (value instanceof Integer integerValue) {
      return integerValue;
    } else if (value instanceof TimestampData timestampData) {
      return (int) (timestampData.getMillisecond() / 1000);
    } else {
      throw new IllegalArgumentException("Unsupported type: " + value.getClass());
    }
  }

  protected void output(RowData leftRow, RowData rightRow, Collector<RowData> collector) {
    returnRowData.replace(leftRow, rightRow);
    collector.collect(returnRowData);
  }

  protected boolean legalTs(Integer ts) {
    return ts != null && ts > 0;
  }

  /**
   * local date time.
   *
   * @param timestamp timestamp
   * @return days
   */
  public LocalDateTime localDateTime(Integer timestamp) {
    return Instant.ofEpochSecond(timestamp).atZone(zoneId).toLocalDateTime();
  }
}
