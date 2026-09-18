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

package name.zicat.astatine.streaming.sql.runtime.test.process.window;

import name.zicat.astatine.streaming.sql.runtime.process.windows.Int2BytesAggregationFunction;
import name.zicat.astatine.streaming.sql.runtime.process.windows.Long2BytesAggregationFunction;
import name.zicat.astatine.streaming.sql.runtime.process.windows.SessionTumble2TumbleWindowFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.data.RowData.createFieldGetter;

/** Tests for {@link SessionTumble2TumbleWindowFunction}. */
public class SessionTumble2TumbleWindowFunctionTest {

  private static final long TUMBLE_INTERVAL_MILLIS = 2_000L;
  private static final long SESSION_DURATION_MILLIS = 5_000L;
  private static final RowType.RowField EVENT_TIME_FIELD =
      new RowType.RowField("ts", new TimestampType(3));
  private static final RowType.RowField[] FIELD_TYPES =
      new RowType.RowField[] {
        new RowType.RowField("id", new VarCharType()), new RowType.RowField("f1", new VarCharType())
      };
  private static final RowType.RowField[] VALUE_FIELDS =
      new RowType.RowField[] {new RowType.RowField("v1", new VarBinaryType())};
  private static final RowType.RowField TIME_SERIES_FIELD =
      new RowType.RowField("time_series", new VarBinaryType());

  @Test
  public void testObjectReuseCorruptsBufferedWindowFields() throws Exception {
    final var function = createFunction();
    final KeySelector<RowData, String> keySelector = row -> row.getString(0).toString();
    try (var harness =
        new KeyedOneInputStreamOperatorTestHarness<String, RowData, RowData>(
            new KeyedProcessOperator<>(function), keySelector, Types.STRING)) {
      harness.getExecutionConfig().enableObjectReuse();
      harness.setup(
          InternalTypeInfo.of(function.returnRowType())
              .createSerializer(harness.getExecutionConfig().getSerializerConfig()));
      harness.open();

      final var reusedInput = input("a", "first", 10, 100);
      harness.processElement(reusedInput, 0L);

      reusedInput.setField(1, StringData.fromString("second"));
      reusedInput.setField(2, TimestampData.fromEpochMillis(2_500L));
      reusedInput.setField(3, values(20));
      reusedInput.setField(4, timeSeries(2_500L));
      harness.processElement(reusedInput, 0L);

      harness.processWatermark(10_000L);

      final var results = outputRecords(harness.getOutput());
      Assert.assertEquals(2, results.size());
      assertWindow(results.get(0), "a", "first", 1_999L, 10, 100L);
      assertWindow(results.get(1), "a", "second", 3_999L, 20, 2_500L);
    }
  }

  private static SessionTumble2TumbleWindowFunction createFunction() {
    return new SessionTumble2TumbleWindowFunction(
        createFieldGetter(new TimestampType(3), 2),
        EVENT_TIME_FIELD,
        new int[] {0, 1},
        FIELD_TYPES,
        new int[] {3},
        VALUE_FIELDS,
        List.of(LogicalTypeRoot.INTEGER),
        4,
        TIME_SERIES_FIELD,
        TUMBLE_INTERVAL_MILLIS,
        SESSION_DURATION_MILLIS);
  }

  private static GenericRowData input(String id, String field, int value, long eventTime) {
    return GenericRowData.of(
        StringData.fromString(id),
        StringData.fromString(field),
        TimestampData.fromEpochMillis(eventTime),
        values(value),
        timeSeries(eventTime));
  }

  private static byte[] values(int value) {
    final var handler = new Int2BytesAggregationFunction();
    return handler.output(handler.accumulate(null, value));
  }

  private static byte[] timeSeries(long eventTime) {
    final var handler = new Long2BytesAggregationFunction();
    return handler.output(handler.accumulate(null, eventTime));
  }

  private static List<RowData> outputRecords(Iterable<Object> output) {
    final var results = new ArrayList<RowData>();
    for (var record : output) {
      if (record instanceof StreamRecord<?> streamRecord) {
        results.add((RowData) streamRecord.getValue());
      }
    }
    return results;
  }

  private static void assertWindow(
      RowData row, String id, String field, long eventTime, int value, long timeSeries) {
    Assert.assertEquals(id, row.getString(0).toString());
    Assert.assertEquals(field, row.getString(1).toString());
    Assert.assertEquals(eventTime, row.getTimestamp(2, 3).getMillisecond());

    final var valueIterator = new Int2BytesAggregationFunction().outputIterator(row.getBinary(3));
    Assert.assertTrue(valueIterator.hasNext());
    Assert.assertEquals(value, valueIterator.next());
    Assert.assertFalse(valueIterator.hasNext());

    final var timeSeriesIterator =
        new Long2BytesAggregationFunction().outputIterator(row.getBinary(4));
    Assert.assertTrue(timeSeriesIterator.hasNext());
    Assert.assertEquals(timeSeries, timeSeriesIterator.next());
    Assert.assertFalse(timeSeriesIterator.hasNext());
  }
}
