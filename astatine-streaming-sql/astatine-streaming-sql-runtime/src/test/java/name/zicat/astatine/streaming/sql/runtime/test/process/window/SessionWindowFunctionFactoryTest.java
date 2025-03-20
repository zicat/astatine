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

import name.zicat.astatine.streaming.sql.parser.function.FunctionFactory;
import name.zicat.astatine.streaming.sql.parser.test.transform.TransformFactoryTestBase;
import name.zicat.astatine.streaming.sql.parser.transform.ProcessTransformFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformContext;
import name.zicat.astatine.streaming.sql.parser.transform.TransformFactory;
import name.zicat.astatine.streaming.sql.runtime.process.windows.SessionWindowFunctionFactory;
import name.zicat.astatine.streaming.sql.runtime.test.utils.TimestampWatermarkGenerator;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.*;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.flink.table.data.TimestampData.fromEpochMillis;

public class SessionWindowFunctionFactoryTest extends TransformFactoryTestBase {
  @Test
  public void testSessionEndAndCreateNew() throws Exception {
    final var ts = System.currentTimeMillis();
    final var context = createCommonContext();
    final var factory =
        TransformFactory.findFactory(ProcessTransformFactory.IDENTITY)
            .cast(ProcessTransformFactory.class);
    final var row1 = GenericRowData.of(StringData.fromString("n1"), fromEpochMillis(ts), 1L);
    final var rowWatermark =
        GenericRowData.of(StringData.fromString("n2"), fromEpochMillis(ts + 60000), 100L);
    final var row2 =
        GenericRowData.of(StringData.fromString("n1"), fromEpochMillis(ts + 182000), 2L);

    final var keySelect =
        new KeySelector<RowData, StringData>() {
          @Override
          public StringData getKey(RowData rowData) {
            return rowData.getString(0);
          }
        };

    final var source =
        env.fromCollection(Arrays.<RowData>asList(row1, rowWatermark, row2))
            .assignTimestampsAndWatermarks(TimestampWatermarkGenerator.create(1))
            .returns(
                InternalTypeInfo.of(
                    new RowType(
                        Arrays.asList(
                            new RowType.RowField("name", new VarCharType()),
                            new RowType.RowField("ts", new TimestampType(3)),
                            new RowType.RowField("score", new BigIntType())))));
    final var result = factory.transform(context, source.keyBy(keySelect));
    TransformFactoryTestBase.execAndAssert(
        result,
        data -> {
          Assert.assertEquals(3, data.size());
          final var resultRow0 = (RowData) data.get(0);
          Assert.assertEquals("n1", resultRow0.getString(0).toString());
          Assert.assertArrayEquals(new Long[] {1L}, parseValue(resultRow0.getBinary(1)));
          Assert.assertEquals(fromEpochMillis(ts + 60000), resultRow0.getTimestamp(2, 3));
          Assert.assertArrayEquals(new Long[] {ts}, parseValue(resultRow0.getBinary(3)));

          final var resultRow1 = (RowData) data.get(2);
          Assert.assertEquals("n1", resultRow1.getString(0).toString());
          Assert.assertArrayEquals(new Long[] {2L}, parseValue(resultRow1.getBinary(1)));
          Assert.assertEquals(fromEpochMillis(ts + 182000 + 60000), resultRow1.getTimestamp(2, 3));
          Assert.assertArrayEquals(new Long[] {ts + 182000}, parseValue(resultRow1.getBinary(3)));
        });
  }

  @Test
  public void testOverNextWindow() throws Exception {
    final var ts = System.currentTimeMillis();
    final var context = createCommonContext();
    final var factory =
        TransformFactory.findFactory(ProcessTransformFactory.IDENTITY)
            .cast(ProcessTransformFactory.class);
    final var row1 = GenericRowData.of(StringData.fromString("n1"), fromEpochMillis(ts), 1L);
    final var row2 =
        GenericRowData.of(StringData.fromString("n1"), fromEpochMillis(ts + 122000), 2L);

    final var keySelect =
        new KeySelector<RowData, StringData>() {
          @Override
          public StringData getKey(RowData rowData) {
            return rowData.getString(0);
          }
        };

    final var source =
        env.fromCollection(Arrays.<RowData>asList(row1, row2))
            .assignTimestampsAndWatermarks(TimestampWatermarkGenerator.create(1))
            .returns(
                InternalTypeInfo.of(
                    new RowType(
                        Arrays.asList(
                            new RowType.RowField("name", new VarCharType()),
                            new RowType.RowField("ts", new TimestampType(3)),
                            new RowType.RowField("score", new BigIntType())))));
    final var result = factory.transform(context, source.keyBy(keySelect));
    TransformFactoryTestBase.execAndAssert(
        result,
        data -> {
          Assert.assertEquals(2, data.size());
          final var resultRow0 = (RowData) data.get(0);
          Assert.assertEquals("n1", resultRow0.getString(0).toString());
          Assert.assertArrayEquals(new Long[] {1L}, parseValue(resultRow0.getBinary(1)));
          Assert.assertEquals(fromEpochMillis(ts + 60000), resultRow0.getTimestamp(2, 3));
          Assert.assertArrayEquals(new Long[] {ts}, parseValue(resultRow0.getBinary(3)));

          final var resultRow1 = (RowData) data.get(1);
          Assert.assertEquals("n1", resultRow1.getString(0).toString());
          Assert.assertArrayEquals(new Long[] {2L}, parseValue(resultRow1.getBinary(1)));
          Assert.assertEquals(fromEpochMillis(ts + 122000 + 60000), resultRow1.getTimestamp(2, 3));
          Assert.assertArrayEquals(new Long[] {ts + 122000}, parseValue(resultRow1.getBinary(3)));
        });
  }

  @Test
  public void testSkip2NextWindow() throws Exception {
    final var ts = System.currentTimeMillis();
    final var context = createCommonContext();
    final var factory =
        TransformFactory.findFactory(ProcessTransformFactory.IDENTITY)
            .cast(ProcessTransformFactory.class);
    final var row1 = GenericRowData.of(StringData.fromString("n1"), fromEpochMillis(ts), 1L);
    final var row2 =
        GenericRowData.of(StringData.fromString("n1"), fromEpochMillis(ts + 62000), 2L);

    final var keySelect =
        new KeySelector<RowData, StringData>() {
          @Override
          public StringData getKey(RowData rowData) {
            return rowData.getString(0);
          }
        };

    final var source =
        env.fromCollection(Arrays.<RowData>asList(row1, row2))
            .assignTimestampsAndWatermarks(TimestampWatermarkGenerator.create(1))
            .returns(
                InternalTypeInfo.of(
                    new RowType(
                        Arrays.asList(
                            new RowType.RowField("name", new VarCharType()),
                            new RowType.RowField("ts", new TimestampType(3)),
                            new RowType.RowField("score", new BigIntType())))));
    final var result = factory.transform(context, source.keyBy(keySelect));
    TransformFactoryTestBase.execAndAssert(
        result,
        data -> {
          Assert.assertEquals(2, data.size());
          final var resultRow0 = (RowData) data.get(0);
          Assert.assertEquals("n1", resultRow0.getString(0).toString());
          Assert.assertArrayEquals(new Long[] {1L}, parseValue(resultRow0.getBinary(1)));
          Assert.assertEquals(fromEpochMillis(ts + 60000), resultRow0.getTimestamp(2, 3));
          Assert.assertArrayEquals(new Long[] {ts}, parseValue(resultRow0.getBinary(3)));

          final var resultRow1 = (RowData) data.get(1);
          Assert.assertEquals("n1", resultRow1.getString(0).toString());
          Assert.assertArrayEquals(new Long[] {2L}, parseValue(resultRow1.getBinary(1)));
          Assert.assertEquals(fromEpochMillis(ts + 120000), resultRow1.getTimestamp(2, 3));
          Assert.assertArrayEquals(new Long[] {ts + 62000}, parseValue(resultRow1.getBinary(3)));
        });
  }

  @Test
  public void testOutputEventTime() throws Exception {
    final var ts = System.currentTimeMillis();
    final var context = createCommonContext();
    final var factory =
        TransformFactory.findFactory(ProcessTransformFactory.IDENTITY)
            .cast(ProcessTransformFactory.class);

    final var row1 = GenericRowData.of(StringData.fromString("n1"), fromEpochMillis(ts), 1L);
    final var row2 =
        GenericRowData.of(StringData.fromString("n1"), fromEpochMillis(ts + 59999), 2L);
    final var row3 =
        GenericRowData.of(StringData.fromString("n1"), fromEpochMillis(ts + 100000), 3L);
    final var row4 =
        GenericRowData.of(StringData.fromString("n1"), fromEpochMillis(ts + 110000), 4L);
    final var row5 =
        GenericRowData.of(StringData.fromString("n1"), fromEpochMillis(ts + 120000), 5L);

    final var keySelect =
        new KeySelector<RowData, StringData>() {
          @Override
          public StringData getKey(RowData rowData) {
            return rowData.getString(0);
          }
        };

    final var source =
        env.fromCollection(Arrays.<RowData>asList(row1, row2, row3, row4, row5))
            .assignTimestampsAndWatermarks(TimestampWatermarkGenerator.create(1))
            .returns(
                InternalTypeInfo.of(
                    new RowType(
                        Arrays.asList(
                            new RowType.RowField("name", new VarCharType()),
                            new RowType.RowField("ts", new TimestampType(3)),
                            new RowType.RowField("score", new BigIntType())))));
    final var result = factory.transform(context, source.keyBy(keySelect));
    TransformFactoryTestBase.execAndAssert(
        result,
        data -> {
          Assert.assertEquals(3, data.size());
          final var resultRow0 = (RowData) data.get(0);
          Assert.assertEquals("n1", resultRow0.getString(0).toString());
          Assert.assertArrayEquals(new Long[] {1L, 2L}, parseValue(resultRow0.getBinary(1)));
          Assert.assertEquals(fromEpochMillis(ts + 60000), resultRow0.getTimestamp(2, 3));
          Assert.assertArrayEquals(
              new Long[] {ts, ts + 59999}, parseValue(resultRow0.getBinary(3)));

          final var resultRow1 = (RowData) data.get(1);
          Assert.assertEquals("n1", resultRow1.getString(0).toString());
          Assert.assertArrayEquals(new Long[] {3L, 4L}, parseValue(resultRow1.getBinary(1)));
          Assert.assertEquals(fromEpochMillis(ts + 120000), resultRow1.getTimestamp(2, 3));
          Assert.assertArrayEquals(
              new Long[] {ts + 100000, ts + 110000}, parseValue(resultRow1.getBinary(3)));

          final var resultRow2 = (RowData) data.get(2);
          Assert.assertEquals("n1", resultRow2.getString(0).toString());
          Assert.assertArrayEquals(new Long[] {5L}, parseValue(resultRow2.getBinary(1)));
          Assert.assertEquals(fromEpochMillis(ts + 180000), resultRow2.getTimestamp(2, 3));
          Assert.assertArrayEquals(new Long[] {ts + 120000}, parseValue(resultRow2.getBinary(3)));
        });
  }

  public static Long[] parseValue(byte[] data) throws IOException {
    final var result = new ArrayList<Long>();
    try (InputStream is = new ByteArrayInputStream(data)) {
      while (is.available() != 0) {
        result.add(readVLong(is));
      }
    }
    result.sort(Long::compareTo);
    return result.toArray(new Long[] {});
  }

  public static Long readVLong(InputStream is) throws IOException {
    long value = 0;
    int shift = 0;
    int aByte;
    while ((aByte = is.read()) != -1) {
      value |= (long) (aByte & 0x7F) << shift;
      if ((aByte & 0x80) == 0) {
        return value;
      }
      shift += 7;
    }
    throw new RuntimeException("read vlong error");
  }

  private TransformContext createCommonContext() {
    final var configuration = new Configuration();
    configuration.set(
        FunctionFactory.OPTION_FUNCTION_IDENTITY, SessionWindowFunctionFactory.IDENTITY);
    configuration.set(SessionWindowFunctionFactory.OPTION_FIELDS, "name");
    configuration.set(SessionWindowFunctionFactory.OPTION_VALUES, "score AS score_1");
    configuration.set(SessionWindowFunctionFactory.OPTION_EVENTTIME, "ts");
    configuration.set(SessionWindowFunctionFactory.OPTION_SESSION_DURATION, Duration.ofMinutes(1));
    return createContext(configuration);
  }
}
