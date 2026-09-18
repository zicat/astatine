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

import name.zicat.astatine.streaming.sql.runtime.process.windows.SessionTumbleWindowFunction;
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
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.flink.table.data.RowData.createFieldGetter;

/** Tests for {@link SessionTumbleWindowFunction}. */
public class SessionTumbleWindowFunctionTest {

  private static final long SESSION_MILLIS = 1_000L;
  private static final RowType.RowField EVENT_TIME_FIELD =
      new RowType.RowField("ts", new TimestampType(3));
  private static final RowType.RowField[] FIELD_TYPES =
      new RowType.RowField[] {new RowType.RowField("name", new VarCharType())};
  private static final RowType.RowField[] VALUE_FIELDS =
      new RowType.RowField[] {new RowType.RowField("score", new BigIntType())};

  @Test
  public void testObjectReuseCorruptsBufferedRows() throws Exception {
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

      final var reusedInput = input("a", 10L, 100L);
      harness.processElement(reusedInput, 0L);

      reusedInput.setField(1, 20L);
      reusedInput.setField(2, TimestampData.fromEpochMillis(2_500L));
      harness.processElement(reusedInput, 0L);

      harness.processWatermark(5_000L);

      final var results =
          harness.getOutput().stream()
              .filter(StreamRecord.class::isInstance)
              .map(record -> ((StreamRecord<RowData>) record).getValue())
              .toList();
      Assert.assertEquals(2, results.size());
      assertSession(results.get(0), "a", 1_100L, 100L, 10L);
      assertSession(results.get(1), "a", 3_500L, 2_500L, 20L);
    }
  }

  private static SessionTumbleWindowFunction createFunction() {
    return new SessionTumbleWindowFunction(
        createFieldGetter(new TimestampType(3), 2),
        EVENT_TIME_FIELD,
        new int[] {0},
        FIELD_TYPES,
        new int[] {1},
        VALUE_FIELDS,
        VALUE_FIELDS,
        "time_series",
        SESSION_MILLIS,
        0L);
  }

  private static GenericRowData input(String name, long score, long eventTime) {
    return GenericRowData.of(
        StringData.fromString(name), score, TimestampData.fromEpochMillis(eventTime));
  }

  private static void assertSession(
      RowData row, String name, long endTime, long eventTime, long score) {
    Assert.assertEquals(name, row.getString(0).toString());
    Assert.assertEquals(endTime, row.getTimestamp(2, 3).getMillisecond());
    Assert.assertArrayEquals(
        new Long[] {eventTime},
        SessionTumbleWindowFunctionFactoryTest.parseValue(row.getBinary(3)));
    Assert.assertArrayEquals(
        new Long[] {score}, SessionTumbleWindowFunctionFactoryTest.parseValue(row.getBinary(1)));
  }
}
