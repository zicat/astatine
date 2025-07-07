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

import name.zicat.astatine.streaming.sql.parser.function.FunctionFactory;
import name.zicat.astatine.streaming.sql.parser.test.transform.TransformFactoryTestBase;
import name.zicat.astatine.streaming.sql.parser.test.transform.WatermarkTransformFactoryTest;
import name.zicat.astatine.streaming.sql.parser.transform.TransformFactory;
import name.zicat.astatine.streaming.sql.parser.transform.WatermarkTransformFactory;
import name.zicat.astatine.streaming.sql.runtime.test.utils.TimestampWatermarkGenerator;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
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

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;

import static name.zicat.astatine.streaming.sql.runtime.process.FieldWatermarkFunctionFactory.*;

/** FieldWatermarkFunctionFactoryTest. */
public class FieldWatermarkFunctionFactoryTest extends TransformFactoryTestBase {

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void test() throws Exception {

    final var ts = System.currentTimeMillis();
    final var delay = Duration.ofSeconds(3);

    final var leftRow1 = new GenericRowData(2);
    leftRow1.setField(0, StringData.fromString("name1"));
    leftRow1.setField(1, TimestampData.fromEpochMillis(ts + 1000));
    final var leftRow11 = new GenericRowData(2);
    leftRow11.setField(0, StringData.fromString("name1"));
    leftRow11.setField(1, TimestampData.fromEpochMillis(ts - 4000));

    final var leftRow2 = new GenericRowData(2);
    leftRow2.setField(0, StringData.fromString("name1"));
    leftRow2.setField(1, TimestampData.fromEpochMillis(ts + 2000));
    final var leftRow3 = new GenericRowData(2);
    leftRow3.setField(0, StringData.fromString("name1"));
    leftRow3.setField(1, TimestampData.fromEpochMillis(ts + 3000));

    final TypeInformation<RowData> leftType =
        InternalTypeInfo.of(
            new RowType(
                Arrays.asList(
                    new RowType.RowField("name", new VarCharType()),
                    new RowType.RowField("ts", new TimestampType(3)))));

    final var source =
        env.fromCollection(Arrays.<RowData>asList(leftRow1, leftRow11, leftRow2, leftRow3))
            .assignTimestampsAndWatermarks(TimestampWatermarkGenerator.create(1))
            .returns(leftType);

    final var configuration = new Configuration();
    configuration.set(FunctionFactory.OPTION_FUNCTION_IDENTITY, IDENTITY);
    configuration.set(OPTION_FIELD, "ts");
    configuration.set(OPTION_DELAY_DURATION, delay);
    configuration.set(OPTION_EMIT_ON_EVENT, true);
    configuration.set(ExecutionConfigOptions.IDLE_STATE_RETENTION, Duration.ofHours(1));

    final var context = createContext(configuration);

    final var factory =
        TransformFactory.findFactory(WatermarkTransformFactory.IDENTITY)
            .cast(WatermarkTransformFactory.class);

    final var resultStream = factory.transform(context, source);

    final var delayMillis = delay.toMillis();
    final Iterator<Long> expectedValuesIt =
        new WatermarkTransformFactoryTest.ExpectValueIterator(
            ts + 1000 - delayMillis,
            ts + 2000 - delayMillis,
            ts + 3000 - delayMillis,
            Long.MAX_VALUE);
    resultStream.addSink(
        new SinkFunction() {

          @Override
          public void writeWatermark(Watermark watermark) {
            Assert.assertEquals(expectedValuesIt.next().longValue(), watermark.getTimestamp());
          }
        });
    resultStream.getExecutionEnvironment().execute();
  }
}
