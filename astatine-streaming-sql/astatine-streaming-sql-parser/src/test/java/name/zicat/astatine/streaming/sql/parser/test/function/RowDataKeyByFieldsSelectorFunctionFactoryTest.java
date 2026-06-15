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

package name.zicat.astatine.streaming.sql.parser.test.function;

import name.zicat.astatine.streaming.sql.parser.function.FunctionFactory;
import name.zicat.astatine.streaming.sql.parser.test.transform.TransformFactoryTestBase;
import name.zicat.astatine.streaming.sql.parser.transform.KeyByTransformFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformFactory;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
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
import java.util.HashMap;
import java.util.List;

import static name.zicat.astatine.streaming.sql.parser.function.RowDataKeyByFieldsSelectorFunctionFactory.IDENTITY;
import static name.zicat.astatine.streaming.sql.parser.function.RowDataKeyByFieldsSelectorFunctionFactory.OPTION_FIELD;

/** RowDataKeyByFieldsSelectorFunctionFactoryTest. */
public class RowDataKeyByFieldsSelectorFunctionFactoryTest extends TransformFactoryTestBase {

  private static final RowType ROW_TYPE =
      new RowType(
          Arrays.asList(
              new RowType.RowField("name", new VarCharType()),
              new RowType.RowField("ts", new TimestampType(3)),
              new RowType.RowField("value", new IntType())));

  @SuppressWarnings({"deprecation", "unchecked"})
  @Test
  public void testKeyBySingleField() throws Exception {
    final var ts = System.currentTimeMillis();
    final var configuration = new Configuration();
    configuration.set(FunctionFactory.OPTION_FUNCTION_IDENTITY, IDENTITY);
    configuration.set(OPTION_FIELD, "name");

    final var context = createContext(configuration);
    final var factory =
        TransformFactory.findFactory(KeyByTransformFactory.IDENTITY)
            .cast(KeyByTransformFactory.class);

    final var row1 = createRow("name1", ts + 1000, 10);
    final var row2 = createRow("name1", ts + 2000, 20);
    final var row3 = createRow("name2", ts + 3000, 5);
    final var row4 = createRow("name2", ts + 4000, 15);
    final var row5 = createRow("name3", ts + 5000, 100);

    final var source =
        env.fromCollection(Arrays.<RowData>asList(row1, row2, row3, row4, row5))
            .returns(InternalTypeInfo.of(ROW_TYPE));

    final var result = factory.transform(context, source);
    Assert.assertTrue(
        "result must be KeyedStream but was " + result.getClass(), result instanceof KeyedStream);

    // reduce by summing values to verify records are grouped by key correctly
    final var keyedStream = (KeyedStream<RowData, ?>) result;
    final var reduced =
        keyedStream.reduce(
            (ReduceFunction<RowData>)
                (v1, v2) -> {
                  final var merged = new GenericRowData(3);
                  merged.setField(0, v1.getString(0));
                  merged.setField(1, v1.getTimestamp(1, 3));
                  merged.setField(2, v1.getInt(2) + v2.getInt(2));
                  return merged;
                });

    execAndAssert(
        reduced,
        results -> {
          // group results by name
          final var sumByName = new HashMap<String, Integer>();
          for (final var row : results) {
            sumByName.put(row.getString(0).toString(), row.getInt(2));
          }
          Assert.assertEquals(Integer.valueOf(30), sumByName.get("name1"));
          Assert.assertEquals(Integer.valueOf(20), sumByName.get("name2"));
          Assert.assertEquals(Integer.valueOf(100), sumByName.get("name3"));
        });
  }

  @SuppressWarnings({"deprecation", "unchecked"})
  @Test
  public void testKeyByMultiFields() throws Exception {
    final var ts = System.currentTimeMillis();
    final var configuration = new Configuration();
    configuration.set(FunctionFactory.OPTION_FUNCTION_IDENTITY, IDENTITY);
    configuration.set(OPTION_FIELD, "name,value");

    final var context = createContext(configuration);
    final var factory =
        TransformFactory.findFactory(KeyByTransformFactory.IDENTITY)
            .cast(KeyByTransformFactory.class);

    // keyBy (name, value): same name but different value -> different key group
    // key=(name1,10): row1(10), row2(10) -> reduce sum = 10+10=20
    // key=(name1,99): row3(99) -> stays 99
    // If keyBy was only on "name", all 3 would merge to 10+10+99=119
    final var row1 = createRow("name1", ts + 1000, 10);
    final var row2 = createRow("name1", ts + 2000, 10);
    final var row3 = createRow("name1", ts + 3000, 99);

    final var source =
        env.fromCollection(Arrays.<RowData>asList(row1, row2, row3))
            .returns(InternalTypeInfo.of(ROW_TYPE));

    final var result = factory.transform(context, source);
    Assert.assertTrue(
        "result must be KeyedStream but was " + result.getClass(), result instanceof KeyedStream);

    final var keyedStream = (KeyedStream<RowData, ?>) result;
    final var reduced =
        keyedStream.reduce(
            (ReduceFunction<RowData>)
                (v1, v2) -> {
                  final var merged = new GenericRowData(3);
                  merged.setField(0, v1.getString(0));
                  merged.setField(1, v1.getTimestamp(1, 3));
                  merged.setField(2, v1.getInt(2) + v2.getInt(2));
                  return merged;
                });

    execAndAssert(
        reduced,
        results -> {
          // reduce emits one output per input, so we collect all emitted values
          // key=(name1,10) group emits: 10, then 20
          // key=(name1,99) group emits: 99
          // If keyBy was only on name, we'd see 10, 20, 119 -> max=119
          // With multi-field keyBy, max should be 99 (not 119)
          int maxValue = 0;
          for (final var row : results) {
            maxValue = Math.max(maxValue, row.getInt(2));
          }
          // proves the (name1,99) group is separate from (name1,10) group
          Assert.assertEquals(99, maxValue);
        });
  }

  @SuppressWarnings({"deprecation"})
  @Test
  public void testKeyByDefaultWithoutFunctionIdentity() {
    final var ts = System.currentTimeMillis();
    // do not set OPTION_FUNCTION_IDENTITY, KeyByTransformFactory should default to IDENTITY
    final var configuration = new Configuration();
    configuration.set(OPTION_FIELD, "name");

    final var context = createContext(configuration);
    final var factory =
        TransformFactory.findFactory(KeyByTransformFactory.IDENTITY)
            .cast(KeyByTransformFactory.class);

    final var row1 = createRow("a", ts, 1);
    final var row2 = createRow("a", ts + 1000, 2);

    final var source =
        env.fromCollection(Arrays.<RowData>asList(row1, row2))
            .returns(InternalTypeInfo.of(ROW_TYPE));

    final var result = factory.transform(context, source);
    Assert.assertTrue("result must be KeyedStream", result instanceof KeyedStream);
  }

  @SuppressWarnings({"deprecation"})
  @Test(expected = IllegalStateException.class)
  public void testKeyByWithoutFieldsThrows() {
    final var configuration = new Configuration();
    configuration.set(FunctionFactory.OPTION_FUNCTION_IDENTITY, IDENTITY);
    // do not set OPTION_FIELD

    final var context = createContext(configuration);
    final var factory =
        TransformFactory.findFactory(KeyByTransformFactory.IDENTITY)
            .cast(KeyByTransformFactory.class);

    final var source =
        env.fromCollection(List.<RowData>of(createRow("name1", System.currentTimeMillis(), 1)))
            .returns(InternalTypeInfo.of(ROW_TYPE));

    factory.transform(context, source);
  }

  private static GenericRowData createRow(String name, long ts, int value) {
    final var row = new GenericRowData(3);
    row.setField(0, StringData.fromString(name));
    row.setField(1, TimestampData.fromEpochMillis(ts));
    row.setField(2, value);
    return row;
  }
}
