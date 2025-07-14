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

import name.zicat.astatine.streaming.sql.parser.utils.Types;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.TimeZone;

/** ExpansionFlatMapFunctionBase. */
public abstract class ExpansionFlatMapFunctionBase extends RichFlatMapFunction<RowData, RowData> {

  private final String timeZoneStr;
  private final Types.UnixFieldGetter startTsFieldGetter;
  private final Types.UnixFieldGetter currentTsFieldGetter;
  private final Types.UnixFieldGetter endTsFieldGetter;

  protected transient JoinedRowData returnRowData;
  protected transient ZoneId zoneId;

  public ExpansionFlatMapFunctionBase(
      String timeZoneStr,
      Types.UnixFieldGetter startTsFieldGetter,
      Types.UnixFieldGetter currentTsFieldGetter,
      Types.UnixFieldGetter endTsFieldGetter) {
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
    final var startTs = startTsFieldGetter.getFieldOrNull(rowData);
    final var currentTs = currentTsFieldGetter.getFieldOrNull(rowData);
    final var endTs = endTsFieldGetter.getFieldOrNull(rowData);
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
