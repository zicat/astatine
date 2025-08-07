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
import name.zicat.astatine.streaming.sql.runtime.process.windows.Int2BytesAggregationFunction;
import name.zicat.astatine.streaming.sql.runtime.process.windows.Long2BytesAggregationFunction;
import name.zicat.astatine.streaming.sql.runtime.process.windows.SessionTumble2TumbleWindowFunctionFactory;
import name.zicat.astatine.streaming.sql.runtime.process.windows.SessionTumbleWindowFunctionFactory;
import name.zicat.astatine.streaming.sql.runtime.test.utils.TimestampWatermarkGenerator;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.*;

import static org.apache.flink.table.data.TimestampData.fromEpochMillis;

/** SessionTumble2TumbleWindowFunctionFactoryTest. */
@SuppressWarnings("unchecked")
public class SessionTumble2TumbleWindowFunctionFactoryTest extends TransformFactoryTestBase {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testOutputWaterMark() throws Exception {
    final var valueHandler = new Int2BytesAggregationFunction();
    final var tumbleInterval = Duration.ofSeconds(2);
    final var streamResult1 =
        testSessionDurationAndTumbleInterval(Duration.ofSeconds(5), tumbleInterval);
    final var resultStream =
        streamResult1
            .keyBy((KeySelector<RowData, StringData>) rowData -> rowData.getString(0)) // key by id
            .window(TumblingEventTimeWindows.of(Time.seconds(tumbleInterval.getSeconds())))
            .process(
                new ProcessWindowFunction<RowData, Integer, StringData, TimeWindow>() {
                  @Override
                  public void process(
                      StringData stringData,
                      ProcessWindowFunction<RowData, Integer, StringData, TimeWindow>.Context
                          context,
                      Iterable<RowData> elements,
                      Collector<Integer> out) {
                    final var it = elements.iterator();
                    int sum = 0;
                    while (it.hasNext()) {
                      final var valueIt = valueHandler.outputIterator(it.next().getBinary(3));
                      while (valueIt.hasNext()) {
                        sum += (int) valueIt.next();
                      }
                    }
                    out.collect(sum);
                  }
                });
    execAndAssert(
        resultStream,
        data -> {
          Assert.assertEquals(15, data.size());
          for (int i = 0; i < data.size(); i++) {
            Assert.assertEquals(i * 4L + 1, (int) data.get(i));
          }
        });
  }

  @Test
  public void test() throws Exception {

    final var firstTumbleStart = 1741582800000L;
    final var valueHandler = new Int2BytesAggregationFunction();
    final var timeSeriesHandler = new Long2BytesAggregationFunction();

    final var tumbleInterval = Duration.ofSeconds(2);
    final var streamResult1 =
        testSessionDurationAndTumbleInterval(Duration.ofSeconds(5), tumbleInterval);
    execAndAssert(
        streamResult1,
        data -> {
          Assert.assertEquals(15, data.size());
          var tsOffset = firstTumbleStart;
          var valueOffset = 0;
          var timeSeriesOffset = firstTumbleStart;
          for (var record : data) {
            Assert.assertEquals("s1", record.getString(0).toString());
            Assert.assertEquals("fv1", record.getString(1).toString());
            Assert.assertEquals(
                tsOffset + tumbleInterval.toMillis() - 1,
                record.getTimestamp(2, 3).getMillisecond());
            tsOffset += tumbleInterval.toMillis();
            final var valueIt = valueHandler.outputIterator(record.getBinary(3));
            var count = 0;
            while (valueIt.hasNext()) {
              Assert.assertEquals(valueOffset, valueIt.next());
              valueOffset++;
              count++;
            }
            Assert.assertEquals(count, 2);
            final var timeSeriesIt = timeSeriesHandler.outputIterator(record.getBinary(4));
            count = 0;
            while (timeSeriesIt.hasNext()) {
              Assert.assertEquals(timeSeriesOffset, timeSeriesIt.next());
              timeSeriesOffset += 1000;
              count++;
            }
            Assert.assertEquals(count, 2);
          }
        });

    final var tumbleInterval2 = Duration.ofSeconds(5);
    final var resultStream2 =
        testSessionDurationAndTumbleInterval(Duration.ofSeconds(5), tumbleInterval2);
    execAndAssert(
        resultStream2,
        data -> {
          Assert.assertEquals(6, data.size());
          var tsOffset = firstTumbleStart;
          var valueOffset = 0;
          var timeSeriesOffset = firstTumbleStart;
          for (var record : data) {
            Assert.assertEquals("s1", record.getString(0).toString());
            Assert.assertEquals("fv1", record.getString(1).toString());
            Assert.assertEquals(
                tsOffset + tumbleInterval2.toMillis() - 1,
                record.getTimestamp(2, 3).getMillisecond());
            tsOffset += tumbleInterval2.toMillis();
            final var valueIt = valueHandler.outputIterator(record.getBinary(3));
            var count = 0;
            while (valueIt.hasNext()) {
              Assert.assertEquals(valueOffset, valueIt.next());
              valueOffset++;
              count++;
            }
            Assert.assertEquals(count, 5);
            final var timeSeriesIt = timeSeriesHandler.outputIterator(record.getBinary(4));
            count = 0;
            while (timeSeriesIt.hasNext()) {
              Assert.assertEquals(timeSeriesOffset, timeSeriesIt.next());
              timeSeriesOffset += 1000;
              count++;
            }
            Assert.assertEquals(count, 5);
          }
        });

    final var tumbleInterval3 = Duration.ofSeconds(10);
    final var resultStream3 =
        testSessionDurationAndTumbleInterval(Duration.ofSeconds(5), tumbleInterval3);

    execAndAssert(
        resultStream3,
        data -> {
          Assert.assertEquals(3, data.size());
          var tsOffset = firstTumbleStart;
          var valueOffset = 0;
          var timeSeriesOffset = firstTumbleStart;
          for (var record : data) {
            Assert.assertEquals("s1", record.getString(0).toString());
            Assert.assertEquals("fv1", record.getString(1).toString());
            Assert.assertEquals(
                tsOffset + tumbleInterval3.toMillis() - 1,
                record.getTimestamp(2, 3).getMillisecond());
            tsOffset += tumbleInterval3.toMillis();
            final var valueIt = valueHandler.outputIterator(record.getBinary(3));
            var count = 0;
            while (valueIt.hasNext()) {
              Assert.assertEquals(valueOffset, valueIt.next());
              valueOffset++;
              count++;
            }
            Assert.assertEquals(count, 10);
            final var timeSeriesIt = timeSeriesHandler.outputIterator(record.getBinary(4));
            count = 0;
            while (timeSeriesIt.hasNext()) {
              Assert.assertEquals(timeSeriesOffset, timeSeriesIt.next());
              timeSeriesOffset += 1000;
              count++;
            }
            Assert.assertEquals(count, 10);
          }
        });
  }

  /**
   * testSessionDurationAndTumbleInterval.
   *
   * @param sessionDuration sessionDuration
   * @param tumbleInterval tumbleInterval
   * @throws Exception Exception
   */
  private DataStream<RowData> testSessionDurationAndTumbleInterval(
      Duration sessionDuration, Duration tumbleInterval) throws Exception {
    final List<RowData> rows = new ArrayList<>();
    final var simpleDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(
                Objects.requireNonNull(
                    Thread.currentThread()
                        .getContextClassLoader()
                        .getResourceAsStream("session_tumble_2_tumble.txt"))))) {
      String line;
      while ((line = reader.readLine()) != null) {
        final var json = (ObjectNode) MAPPER.readTree(line);
        rows.add(
            GenericRowData.of(
                StringData.fromString(json.get("id").asText()),
                StringData.fromString(json.get("f1").asText()),
                fromEpochMillis(simpleDateFormat.parse(json.get("ts").asText()).getTime()),
                json.get("v1").asInt()));
      }
    }

    final var keySelect =
        new KeySelector<RowData, StringData>() {
          @Override
          public StringData getKey(RowData rowData) {
            return rowData.getString(0);
          }
        };

    final var source =
        env.fromCollection(rows)
            .assignTimestampsAndWatermarks(TimestampWatermarkGenerator.create(2))
            .returns(
                InternalTypeInfo.of(
                    new RowType(
                        Arrays.asList(
                            new RowType.RowField("id", new VarCharType()),
                            new RowType.RowField("f1", new VarCharType()),
                            new RowType.RowField("ts", new TimestampType(3)),
                            new RowType.RowField("v1", new IntType())))));

    final var sessionContext = createSessionContext(sessionDuration);
    final var factory =
        TransformFactory.findFactory(ProcessTransformFactory.IDENTITY)
            .cast(ProcessTransformFactory.class);

    final DataStream<RowData> sessionStream =
        (DataStream<RowData>) factory.transform(sessionContext, source.keyBy(keySelect));

    final var session2TumbleContext = createSessionTumbleContext(sessionDuration, tumbleInterval);
    return (DataStream<RowData>)
        factory.transform(session2TumbleContext, sessionStream.keyBy(keySelect));
  }

  /**
   * createSessionContext.
   *
   * @param sessionDuration sessionDuration
   * @return TransformContext
   */
  private TransformContext createSessionContext(Duration sessionDuration) {
    final var configuration = new Configuration();
    configuration.set(
        FunctionFactory.OPTION_FUNCTION_IDENTITY, SessionTumbleWindowFunctionFactory.IDENTITY);
    configuration.set(SessionTumbleWindowFunctionFactory.OPTION_FIELDS, "id,f1");
    configuration.set(SessionTumbleWindowFunctionFactory.OPTION_VALUES, "v1");
    configuration.set(SessionTumbleWindowFunctionFactory.OPTION_EVENTTIME, "ts");
    configuration.set(SessionTumbleWindowFunctionFactory.OPTION_SESSION_DURATION, sessionDuration);
    return createContext(configuration);
  }

  /**
   * createSessionTumbleContext.
   *
   * @param sessionDuration sessionDuration
   * @return TransformContext
   */
  private TransformContext createSessionTumbleContext(
      Duration sessionDuration, Duration tumbleInterval) {
    final var configuration = new Configuration();
    configuration.set(
        FunctionFactory.OPTION_FUNCTION_IDENTITY,
        SessionTumble2TumbleWindowFunctionFactory.IDENTITY);
    configuration.set(SessionTumble2TumbleWindowFunctionFactory.OPTION_FIELDS, "id,f1");
    configuration.set(SessionTumble2TumbleWindowFunctionFactory.OPTION_VALUES, "v1");
    configuration.set(SessionTumble2TumbleWindowFunctionFactory.OPTION_EVENTTIME, "ts");
    configuration.set(
        SessionTumble2TumbleWindowFunctionFactory.OPTION_SESSION_DURATION, sessionDuration);
    configuration.set(
        SessionTumble2TumbleWindowFunctionFactory.OPTION_TUMBLE_INTERVAL, tumbleInterval);
    configuration.set(SessionTumble2TumbleWindowFunctionFactory.OPTION_VALUES_ORIGIN_TYPE, "INT");
    return createContext(configuration);
  }
}
