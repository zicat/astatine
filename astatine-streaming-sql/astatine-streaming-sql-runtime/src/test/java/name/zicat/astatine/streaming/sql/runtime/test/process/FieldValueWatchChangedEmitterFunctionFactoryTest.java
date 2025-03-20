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
import name.zicat.astatine.streaming.sql.parser.transform.ProcessTransformFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformFactory;
import name.zicat.astatine.streaming.sql.runtime.test.utils.TimestampWatermarkGenerator;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
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

import java.time.Duration;
import java.util.Arrays;

import static name.zicat.astatine.streaming.sql.runtime.process.FieldValueWatchChangedEmitterFunctionFactory.*;

/** FieldValueWatchChangedEmitterFunctionFactoryTest. */
public class FieldValueWatchChangedEmitterFunctionFactoryTest extends TransformFactoryTestBase {

  @Test
  public void test() throws Exception {

    final var ts = System.currentTimeMillis();
    final var configuration = new Configuration();
    configuration.set(WATCH_FIELD, "value");
    configuration.set(OPTION_EVENT_TIME, "ts");
    configuration.set(FunctionFactory.OPTION_FUNCTION_IDENTITY, IDENTIFY);
    configuration.set(ExecutionConfigOptions.IDLE_STATE_RETENTION, Duration.ofSeconds(10));

    final var context = createContext(configuration);
    final var factory =
        TransformFactory.findFactory(ProcessTransformFactory.IDENTITY)
            .cast(ProcessTransformFactory.class);

    final var row1 = new GenericRowData(3);
    row1.setField(0, StringData.fromString("name1"));
    row1.setField(1, TimestampData.fromEpochMillis(ts + 1000));
    row1.setField(2, 1);
    final var row2 = new GenericRowData(3);
    row2.setField(0, StringData.fromString("name1"));
    row2.setField(1, TimestampData.fromEpochMillis(ts + 2000));
    row2.setField(2, 1);
    final var row3 = new GenericRowData(3);
    row3.setField(0, StringData.fromString("name1"));
    row3.setField(1, TimestampData.fromEpochMillis(ts + 60000));
    row3.setField(2, 1);
    final var row4 = new GenericRowData(4);
    row4.setField(0, StringData.fromString("name1"));
    row4.setField(1, TimestampData.fromEpochMillis(ts + 61000));
    row4.setField(2, 2);

    final var source =
        env.fromCollection(Arrays.<RowData>asList(row1, row2, row3, row4))
            .assignTimestampsAndWatermarks(TimestampWatermarkGenerator.create(1))
            .returns(
                InternalTypeInfo.of(
                    new RowType(
                        Arrays.asList(
                            new RowType.RowField("name", new VarCharType()),
                            new RowType.RowField("ts", new TimestampType(3)),
                            new RowType.RowField("value", new IntType())))));

    final var result =
        factory.transform(
            context,
            source.keyBy((KeySelector<RowData, StringData>) rowData -> rowData.getString(0)));
    TransformFactoryTestBase.execAndAssert(
        result,
        data -> {
          Assert.assertEquals(3, data.size());
          final var resultRow0 = (RowData) data.get(0);
          Assert.assertEquals(resultRow0.getString(0).toString(), "name1");
          Assert.assertEquals(resultRow0.getTimestamp(1, 3).getMillisecond(), ts + 1000L);
          Assert.assertEquals(resultRow0.getInt(2), 1);

          final var resultRow1 = (RowData) data.get(1);
          Assert.assertEquals(resultRow1.getString(0).toString(), "name1");
          Assert.assertEquals(resultRow1.getTimestamp(1, 3).getMillisecond(), ts + 60000L);
          Assert.assertEquals(resultRow1.getInt(2), 1);

          final var resultRow2 = (RowData) data.get(2);
          Assert.assertEquals(resultRow2.getString(0).toString(), "name1");
          Assert.assertEquals(resultRow2.getTimestamp(1, 3).getMillisecond(), ts + 61000L);
          Assert.assertEquals(resultRow2.getInt(2), 2);
        });
  }
}
