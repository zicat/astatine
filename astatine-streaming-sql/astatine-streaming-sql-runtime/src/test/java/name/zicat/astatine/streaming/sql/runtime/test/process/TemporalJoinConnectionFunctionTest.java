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

package name.zicat.astatine.streaming.sql.runtime.test.process;

import name.zicat.astatine.streaming.sql.runtime.process.TemporalJoinConnectionFunction;
import name.zicat.astatine.streaming.sql.runtime.process.TemporalJoinConnectionFunctionFactory;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static name.zicat.astatine.streaming.sql.parser.utils.Types.fieldGetter;

/** Tests for {@link TemporalJoinConnectionFunction}. */
public class TemporalJoinConnectionFunctionTest {

  private static final RowType LEFT_ROW_TYPE =
      new RowType(
          Arrays.asList(
              new RowType.RowField("name", new VarCharType()),
              new RowType.RowField("ts", new TimestampType(3)),
              new RowType.RowField("tag", new VarCharType())));
  private static final RowType RIGHT_ROW_TYPE =
      new RowType(
          Arrays.asList(
              new RowType.RowField("name", new VarCharType()),
              new RowType.RowField("ts", new TimestampType(3)),
              new RowType.RowField("tag", new VarCharType())));
  private static final RowType LEFT_RETURN_ROW_TYPE = LEFT_ROW_TYPE;
  private static final RowType RIGHT_RETURN_ROW_TYPE =
      new RowType(Arrays.asList(new RowType.RowField("right_tag", new VarCharType())));

  @Test
  public void testObjectReuseCorruptsBufferedLeftRows() throws Exception {
    final var function =
        new TemporalJoinConnectionFunction<String>(
            InternalTypeInfo.of(LEFT_RETURN_ROW_TYPE),
            InternalTypeInfo.of(RIGHT_RETURN_ROW_TYPE),
            fieldGetter(LEFT_ROW_TYPE, "ts"),
            fieldGetter(RIGHT_ROW_TYPE, "ts"),
            10_000L,
            15_000L,
            TemporalJoinConnectionFunctionFactory.JoinType.INNER,
            new int[] {0, 1, 2},
            new int[] {2},
            TemporalJoinConnectionFunctionFactory.Order.LAST);
    final KeySelector<RowData, String> keySelector = row -> row.getString(0).toString();

    try (var harness =
        new KeyedTwoInputStreamOperatorTestHarness<String, RowData, RowData, RowData>(
            new KeyedCoProcessOperator<>(function),
            keySelector,
            keySelector,
            Types.STRING)) {
      harness.getExecutionConfig().enableObjectReuse();
      harness.setup(
          InternalTypeInfo.of(
                  new RowType(
                      Arrays.asList(
                          new RowType.RowField("name", new VarCharType()),
                          new RowType.RowField("ts", new TimestampType(3)),
                          new RowType.RowField("tag", new VarCharType()),
                          new RowType.RowField("right_tag", new VarCharType()))))
              .createSerializer(harness.getExecutionConfig().getSerializerConfig()));
      harness.open();

      harness.processElement2(rightInput("a", 0L, "rightTag"), 0L);

      final var reusedInput = leftInput("a", 100L, "leftTag1");
      harness.processElement1(reusedInput, 0L);

      reusedInput.setField(1, TimestampData.fromEpochMillis(2_500L));
      reusedInput.setField(2, StringData.fromString("leftTag2"));
      harness.processElement1(reusedInput, 0L);

      harness.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(5_000L));

      final var results =
          harness.getOutput().stream()
              .filter(StreamRecord.class::isInstance)
              .map(record -> ((StreamRecord<RowData>) record).getValue())
              .toList();
      Assert.assertEquals(2, results.size());
      assertJoinedRow(results.get(0), 100L, "leftTag1", "rightTag");
      assertJoinedRow(results.get(1), 2_500L, "leftTag2", "rightTag");
    }
  }

  private static void assertJoinedRow(
      RowData row, long eventTime, String leftTag, String rightTag) {
    Assert.assertEquals(eventTime, row.getTimestamp(1, 3).getMillisecond());
    Assert.assertEquals(leftTag, row.getString(2).toString());
    Assert.assertEquals(rightTag, row.getString(3).toString());
  }

  private static GenericRowData leftInput(String name, long eventTime, String tag) {
    return GenericRowData.of(
        StringData.fromString(name),
        TimestampData.fromEpochMillis(eventTime),
        StringData.fromString(tag));
  }

  private static GenericRowData rightInput(String name, long eventTime, String tag) {
    return GenericRowData.of(
        StringData.fromString(name),
        TimestampData.fromEpochMillis(eventTime),
        StringData.fromString(tag));
  }
}
