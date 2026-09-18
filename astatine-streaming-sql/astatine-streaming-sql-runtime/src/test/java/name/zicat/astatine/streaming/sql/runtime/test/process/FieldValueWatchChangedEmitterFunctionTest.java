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

import name.zicat.astatine.streaming.sql.runtime.process.FieldValueWatchChangedEmitterFunction;
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
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static name.zicat.astatine.streaming.sql.parser.utils.Types.fieldGetter;

/** Tests for {@link FieldValueWatchChangedEmitterFunction}. */
public class FieldValueWatchChangedEmitterFunctionTest {

  private static final RowType ROW_TYPE =
      new RowType(
          Arrays.asList(
              new RowType.RowField("name", new VarCharType()),
              new RowType.RowField("ts", new TimestampType(3)),
              new RowType.RowField("value", new IntType())));

  @Test
  public void testObjectReuseCorruptsBufferedRows() throws Exception {
    final var function =
        new FieldValueWatchChangedEmitterFunction<String>(
            fieldGetter(ROW_TYPE, "value"),
            ROW_TYPE.getFields().get(2),
            fieldGetter(ROW_TYPE, "ts"),
            10_000L,
            15_000L,
            InternalTypeInfo.of(ROW_TYPE));
    final KeySelector<RowData, String> keySelector = row -> row.getString(0).toString();

    try (var harness =
        new KeyedOneInputStreamOperatorTestHarness<String, RowData, RowData>(
            new KeyedProcessOperator<>(function), keySelector, Types.STRING)) {
      harness.getExecutionConfig().enableObjectReuse();
      harness.setup(
          InternalTypeInfo.of(ROW_TYPE)
              .createSerializer(harness.getExecutionConfig().getSerializerConfig()));
      harness.open();

      final var reusedInput = input("a", 100L, 10);
      harness.processElement(reusedInput, 0L);

      reusedInput.setField(1, TimestampData.fromEpochMillis(2_500L));
      reusedInput.setField(2, 20);
      harness.processElement(reusedInput, 0L);

      harness.processWatermark(5_000L);

      final var results =
          harness.getOutput().stream()
              .filter(StreamRecord.class::isInstance)
              .map(record -> ((StreamRecord<RowData>) record).getValue())
              .toList();
      Assert.assertEquals(2, results.size());
      assertRow(results.get(0), "a", 100L, 10);
      assertRow(results.get(1), "a", 2_500L, 20);
    }
  }

  private static void assertRow(RowData row, String name, long eventTime, int value) {
    Assert.assertEquals(name, row.getString(0).toString());
    Assert.assertEquals(eventTime, row.getTimestamp(1, 3).getMillisecond());
    Assert.assertEquals(value, row.getInt(2));
  }

  private static GenericRowData input(String name, long eventTime, int value) {
    return GenericRowData.of(
        StringData.fromString(name), TimestampData.fromEpochMillis(eventTime), value);
  }
}
