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
import name.zicat.astatine.streaming.sql.runtime.process.DeduplicateFunctionFactory;
import name.zicat.astatine.streaming.sql.runtime.test.utils.TimestampWatermarkGenerator;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
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

/** DeduplicateFunctionFactoryTest. */
public class DeduplicateFunctionFactoryTest extends TransformFactoryTestBase {

  @Test
  public void testDesc() throws Exception {
    final var ts = System.currentTimeMillis();
    final var dataStream = test(ts, DeduplicateFunctionFactory.OrderType.DESC);
    execAndAssert(
        dataStream,
        data -> {
          Assert.assertEquals(4, data.size());
          for (int i = 0; i < data.size(); i++) {
            final var rowData = (RowData) data.get(i);
            Assert.assertEquals(rowData.getString(0).toString(), "n1");
            Assert.assertEquals(rowData.getInt(2), i + 1);
          }
        });
  }

  @Test
  public void testASC() throws Exception {
    final var ts = System.currentTimeMillis();
    final var dataStream = test(ts, DeduplicateFunctionFactory.OrderType.ASC);
    execAndAssert(
        dataStream,
        data -> {
          Assert.assertEquals(2, data.size());
          final var rowData = (RowData) data.get(0);
          Assert.assertEquals(rowData.getString(0).toString(), "n1");
          Assert.assertEquals(rowData.getTimestamp(1, 3).getMillisecond(), ts + 1000L);
          Assert.assertEquals(rowData.getInt(2), 1);

          final var rowData2 = (RowData) data.get(1);
          Assert.assertEquals(rowData2.getString(0).toString(), "n1");
          Assert.assertEquals(rowData2.getTimestamp(1, 3).getMillisecond(), ts + 30000L);
          Assert.assertEquals(rowData2.getInt(2), 3);
        });
  }

  private DataStream<?> test(long ts, DeduplicateFunctionFactory.OrderType orderType) {
    final var configuration = new Configuration();
    configuration.set(DeduplicateFunctionFactory.OPTION_EVENT_TIME, "ts");
    configuration.set(DeduplicateFunctionFactory.OPTION_ORDER_TYPE, orderType);
    configuration.set(
        FunctionFactory.OPTION_FUNCTION_IDENTITY, DeduplicateFunctionFactory.IDENTIFY);
    configuration.set(ExecutionConfigOptions.IDLE_STATE_RETENTION, Duration.ofSeconds(10));

    final var context = createContext(configuration);
    final var factory =
        TransformFactory.findFactory(ProcessTransformFactory.IDENTITY)
            .cast(ProcessTransformFactory.class);

    final var row1 = new GenericRowData(3);
    row1.setField(0, StringData.fromString("n1"));
    row1.setField(1, TimestampData.fromEpochMillis(ts + 1000));
    row1.setField(2, 1);
    final var row2 = new GenericRowData(3);
    row2.setField(0, StringData.fromString("n1"));
    row2.setField(1, TimestampData.fromEpochMillis(ts + 2000));
    row2.setField(2, 2);
    final var row3 = new GenericRowData(3);
    row3.setField(0, StringData.fromString("n1"));
    row3.setField(1, TimestampData.fromEpochMillis(ts + 30000));
    row3.setField(2, 3);
    final var row4 = new GenericRowData(3);
    row4.setField(0, StringData.fromString("n1"));
    row4.setField(1, TimestampData.fromEpochMillis(ts + 32000));
    row4.setField(2, 4);

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

    return factory.transform(
        context, source.keyBy((KeySelector<RowData, StringData>) rowData -> rowData.getString(0)));
  }
}
