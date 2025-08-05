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
import name.zicat.astatine.streaming.sql.runtime.process.windows.SessionTumbleWindowFunctionFactory;
import name.zicat.astatine.streaming.sql.runtime.test.utils.TimestampWatermarkGenerator;
import name.zicat.astatine.streaming.sql.runtime.utils.VLongUtils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.*;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;

import static name.zicat.astatine.streaming.sql.runtime.utils.VLongUtils.vLongDecode;
import static org.apache.flink.table.data.TimestampData.fromEpochMillis;

public class SessionTumbleWindowFunctionFactoryTest extends TransformFactoryTestBase {

  @Test
  public void testSupportTypes() throws Exception {
    final var ts = System.currentTimeMillis();
    final var rowType =
        new RowType(
            Arrays.asList(
                new RowType.RowField("name", new VarCharType()),
                new RowType.RowField("ts", new TimestampType(3)),
                new RowType.RowField("score1", new BinaryType()),
                new RowType.RowField("score2", new BooleanType()),
                new RowType.RowField("score3", new TinyIntType()),
                new RowType.RowField("score4", new DoubleType()),
                new RowType.RowField("score5", new FloatType()),
                new RowType.RowField("score6", new IntType()),
                new RowType.RowField("score7", new BigIntType()),
                new RowType.RowField("score8", new SmallIntType()),
                new RowType.RowField("score9", new VarCharType())));

    final var configuration = new Configuration();
    configuration.set(
        FunctionFactory.OPTION_FUNCTION_IDENTITY, SessionTumbleWindowFunctionFactory.IDENTITY);
    configuration.set(SessionTumbleWindowFunctionFactory.OPTION_FIELDS, "name");
    configuration.set(
        SessionTumbleWindowFunctionFactory.OPTION_VALUES,
        "score1,score2,score3,score4,score5,score6,score7,score8,score9");
    configuration.set(SessionTumbleWindowFunctionFactory.OPTION_EVENTTIME, "ts");
    configuration.set(
        SessionTumbleWindowFunctionFactory.OPTION_SESSION_DURATION, Duration.ofMinutes(1));
    final var context = createContext(configuration);
    final var factory =
        TransformFactory.findFactory(ProcessTransformFactory.IDENTITY)
            .cast(ProcessTransformFactory.class);

    final var row1 =
        GenericRowData.of(
            StringData.fromString("n1"),
            fromEpochMillis(ts),
            "binary0".getBytes(),
            true,
            (byte) 1,
            1.0d,
            2.0f,
            4,
            5L,
            (short) 6,
            StringData.fromString("varchar0"));
    final var row2 =
        GenericRowData.of(
            StringData.fromString("n1"),
            fromEpochMillis(ts + 1),
            "binary1".getBytes(),
            false,
            (byte) 2,
            2.0d,
            3.0f,
            5,
            6L,
            (short) 7,
            StringData.fromString("varchar1"));
    final var row3 =
        GenericRowData.of(
            StringData.fromString("n1"),
            fromEpochMillis(ts + 2),
            "binary2".getBytes(),
            true,
            (byte) 3,
            3.0d,
            4.0f,
            6,
            7L,
            (short) 8,
            StringData.fromString("varchar2"));
    final var row4 =
        GenericRowData.of(
            StringData.fromString("n1"),
            fromEpochMillis(ts + 3),
            "binary3".getBytes(),
            false,
            (byte) 4,
            4.0d,
            5.0f,
            7,
            8L,
            (short) 9,
            StringData.fromString("varchar3"));
    final var row5 =
        GenericRowData.of(
            StringData.fromString("n1"),
            fromEpochMillis(ts + 4),
            "binary4".getBytes(),
            true,
            (byte) 5,
            5.0d,
            6.0f,
            8,
            9L,
            (short) 10,
            StringData.fromString("varchar4"));
    final var row6 =
        GenericRowData.of(
            StringData.fromString("n1"),
            fromEpochMillis(ts + 4),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);

    final var keySelect =
        new KeySelector<RowData, StringData>() {
          @Override
          public StringData getKey(RowData rowData) {
            return rowData.getString(0);
          }
        };
    final var source =
        env.fromCollection(Arrays.<RowData>asList(row1, row2, row3, row4, row5, row6))
            .assignTimestampsAndWatermarks(TimestampWatermarkGenerator.create(1))
            .returns(InternalTypeInfo.of(rowType));
    final var result = factory.transform(context, source.keyBy(keySelect));
    TransformFactoryTestBase.execAndAssert(
        result,
        data -> {
          Assert.assertEquals(1, data.size());
          final var row = (RowData) data.get(0);
          final var score1 = ByteBuffer.wrap(row.getBinary(1));
          int offset = 0;
          while (score1.hasRemaining()) {
            final int size = (int) vLongDecode(score1);
            if (!score1.hasRemaining()) {
              Assert.assertEquals(0, size);
              break;
            }
            final byte[] body = new byte[size];
            score1.get(body);
            Assert.assertEquals("binary" + offset, new String(body));
            offset++;
          }

          final var score2 = ByteBuffer.wrap(row.getBinary(2));
          offset = 0;
          while (score2.hasRemaining()) {
            final int value = score2.get();
            if (!score2.hasRemaining()) {
              Assert.assertEquals(0, value);
              break;
            }
            Assert.assertTrue(offset % 2 == 0 ? value == 1 : value == 0);
            offset++;
          }

          final var score3 = ByteBuffer.wrap(row.getBinary(3));
          offset = 0;
          while (score3.hasRemaining()) {
            final byte value = score3.get();
            if (!score3.hasRemaining()) {
              Assert.assertEquals(0, value);
              break;
            }
            Assert.assertEquals(offset + 1, value);
            offset++;
          }

          final var score4 = ByteBuffer.wrap(row.getBinary(4));
          offset = 0;
          while (score4.hasRemaining()) {
            final double value = Double.longBitsToDouble(score4.getLong());
            if (!score4.hasRemaining()) {
              Assert.assertEquals(0d, value, 0.01);
              break;
            }
            Assert.assertEquals(offset + 1d, value, 0.01);
            offset++;
          }

          final var score5 = ByteBuffer.wrap(row.getBinary(5));
          offset = 0;
          while (score5.hasRemaining()) {
            final float value = Float.intBitsToFloat(score5.getInt());
            if (!score5.hasRemaining()) {
              Assert.assertEquals(0f, value, 0.01);
              break;
            }
            Assert.assertEquals(offset + 2f, value, 0.01);
            offset++;
          }

          final var score6 = ByteBuffer.wrap(row.getBinary(6));
          offset = 0;
          while (score6.hasRemaining()) {
            final int value = score6.getInt();
            if (!score6.hasRemaining()) {
              Assert.assertEquals(0, value);
              break;
            }
            Assert.assertEquals(offset + 4, value);
            offset++;
          }

          final var score7 = ByteBuffer.wrap(row.getBinary(7));
          offset = 0;
          while (score7.hasRemaining()) {
            final long value = score7.getLong();
            if (!score7.hasRemaining()) {
              Assert.assertEquals(0, value);
              break;
            }
            Assert.assertEquals(offset + 5L, value);
            offset++;
          }

          final var score8 = ByteBuffer.wrap(row.getBinary(8));
          offset = 0;
          while (score8.hasRemaining()) {
            final short value = score8.getShort();
            if (!score8.hasRemaining()) {
              Assert.assertEquals(0, value);
              break;
            }
            Assert.assertEquals(offset + 6L, value);
            offset++;
          }

          final var score9 = ByteBuffer.wrap(row.getBinary(9));
          offset = 0;
          while (score9.hasRemaining()) {
            final int length = (int) VLongUtils.vLongDecode(score9);
            final byte[] bs = new byte[length];
            score9.get(bs);
            if (!score9.hasRemaining()) {
              Assert.assertEquals(0, bs.length);
              break;
            }
            Assert.assertEquals("varchar" + offset, new String(bs));
            offset++;
          }
        });
  }

  @Test
  public void testFieldChanged() throws Exception {
    final var ts = System.currentTimeMillis();

    final var configuration = new Configuration();
    configuration.set(
        FunctionFactory.OPTION_FUNCTION_IDENTITY, SessionTumbleWindowFunctionFactory.IDENTITY);
    configuration.set(SessionTumbleWindowFunctionFactory.OPTION_FIELDS, "name,state");
    configuration.set(SessionTumbleWindowFunctionFactory.OPTION_VALUES, "score AS score_1");
    configuration.set(SessionTumbleWindowFunctionFactory.OPTION_EVENTTIME, "ts");
    configuration.set(
        SessionTumbleWindowFunctionFactory.OPTION_SESSION_DURATION, Duration.ofMinutes(1));
    final var context = createContext(configuration);

    final var factory =
        TransformFactory.findFactory(ProcessTransformFactory.IDENTITY)
            .cast(ProcessTransformFactory.class);
    /*
      Input:             (t, t+59s, t+100s, t+110, t+120)
      Expected Output    (t+60s, t+120s, t+180s)
    */
    final var row1 = GenericRowData.of(StringData.fromString("n1"), 10, fromEpochMillis(ts), 1L);
    final var row2 =
        GenericRowData.of(StringData.fromString("n1"), 20, fromEpochMillis(ts + 59999), 2L);
    final var row3 =
        GenericRowData.of(StringData.fromString("n1"), 30, fromEpochMillis(ts + 100000), 3L);
    final var row4 =
        GenericRowData.of(StringData.fromString("n1"), 40, fromEpochMillis(ts + 110000), 4L);
    final var row5 =
        GenericRowData.of(StringData.fromString("n1"), 50, fromEpochMillis(ts + 120000), 5L);

    final var keySelect =
        new KeySelector<RowData, StringData>() {
          @Override
          public StringData getKey(RowData rowData) {
            return rowData.getString(0);
          }
        };

    final var source =
        env.fromCollection(Arrays.<RowData>asList(row1, row2, row3, row4, row5))
            .assignTimestampsAndWatermarks(TimestampWatermarkGenerator.create(2))
            .returns(
                InternalTypeInfo.of(
                    new RowType(
                        Arrays.asList(
                            new RowType.RowField("name", new VarCharType()),
                            new RowType.RowField("state", new IntType()),
                            new RowType.RowField("ts", new TimestampType(3)),
                            new RowType.RowField("score", new BigIntType())))));
    final var result = factory.transform(context, source.keyBy(keySelect));
    TransformFactoryTestBase.execAndAssert(
        result,
        data -> {
          Assert.assertEquals(3, data.size());
          final var resultRow0 = (RowData) data.get(0);
          Assert.assertEquals("n1", resultRow0.getString(0).toString());
          Assert.assertEquals(10, resultRow0.getInt(1));
          Assert.assertArrayEquals(new Long[] {1L, 2L}, parseValue(resultRow0.getBinary(2)));
          Assert.assertEquals(fromEpochMillis(ts + 60000), resultRow0.getTimestamp(3, 3));
          Assert.assertArrayEquals(
              new Long[] {ts, ts + 59999}, parseValue(resultRow0.getBinary(4)));

          final var resultRow1 = (RowData) data.get(1);
          Assert.assertEquals("n1", resultRow1.getString(0).toString());
          Assert.assertEquals(30, resultRow1.getInt(1));
          Assert.assertArrayEquals(new Long[] {3L, 4L}, parseValue(resultRow1.getBinary(2)));
          Assert.assertEquals(fromEpochMillis(ts + 120000), resultRow1.getTimestamp(3, 3));
          Assert.assertArrayEquals(
              new Long[] {ts + 100000, ts + 110000}, parseValue(resultRow1.getBinary(4)));

          final var resultRow2 = (RowData) data.get(2);
          Assert.assertEquals("n1", resultRow2.getString(0).toString());
          Assert.assertEquals(50, resultRow2.getInt(1));
          Assert.assertArrayEquals(new Long[] {5L}, parseValue(resultRow2.getBinary(2)));
          Assert.assertEquals(fromEpochMillis(ts + 180000), resultRow2.getTimestamp(3, 3));
          Assert.assertArrayEquals(new Long[] {ts + 120000}, parseValue(resultRow2.getBinary(4)));
        });
  }

  @Test
  public void testSessionEndAndCreateNew() throws Exception {
    final var ts = System.currentTimeMillis();
    final var context = createCommonContext();
    final var factory =
        TransformFactory.findFactory(ProcessTransformFactory.IDENTITY)
            .cast(ProcessTransformFactory.class);

    /*
      Input:             (t, t+182)
      Expected Output    (t+60s, t+182s+60s)
    */

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

    /*
      Input:             (t, t+122)
      Expected Output    (t+60s, t+122s+60s)
    */
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
    /*
      Input:             (t, t+62)
      Expected Output    (t+60s, t+120s)
    */
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

  public static Long[] parseValue(byte[] data) {
    final var result = new ArrayList<Long>();
    final ByteBuffer byteBuffer = ByteBuffer.wrap(data);
    while (byteBuffer.hasRemaining()) {
      result.add(byteBuffer.getLong());
    }
    result.sort(Long::compareTo);
    return result.toArray(new Long[] {});
  }

  private TransformContext createCommonContext() {
    final var configuration = new Configuration();
    configuration.set(
        FunctionFactory.OPTION_FUNCTION_IDENTITY, SessionTumbleWindowFunctionFactory.IDENTITY);
    configuration.set(SessionTumbleWindowFunctionFactory.OPTION_FIELDS, "name");
    configuration.set(SessionTumbleWindowFunctionFactory.OPTION_VALUES, "score AS score_1");
    configuration.set(SessionTumbleWindowFunctionFactory.OPTION_EVENTTIME, "ts");
    configuration.set(
        SessionTumbleWindowFunctionFactory.OPTION_SESSION_DURATION, Duration.ofMinutes(1));
    return createContext(configuration);
  }
}
