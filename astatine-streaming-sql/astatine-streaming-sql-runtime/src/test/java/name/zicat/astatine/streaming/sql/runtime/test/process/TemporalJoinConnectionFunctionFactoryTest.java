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

import name.zicat.astatine.streaming.sql.parser.transform.TransformFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TwoTransformFactory;
import name.zicat.astatine.streaming.sql.runtime.test.utils.TimestampWatermarkGenerator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataTypeQueryable;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;

import static name.zicat.astatine.streaming.sql.runtime.process.TemporalJoinConnectionFunctionFactory.*;

/** TemporalJoinConnectionFunctionFactoryTest. */
public class TemporalJoinConnectionFunctionFactoryTest extends TransformFactoryTestBase {

  @Test
  public void test() throws Exception {

    final var ts = System.currentTimeMillis();

    final var leftRow1 = new GenericRowData(3);
    leftRow1.setField(0, StringData.fromString("name1"));
    leftRow1.setField(1, TimestampData.fromEpochMillis(ts + 1000));
    leftRow1.setField(2, StringData.fromString("leftTag1"));
    final var leftRow2 = new GenericRowData(3);
    leftRow2.setField(0, StringData.fromString("name1"));
    leftRow2.setField(1, TimestampData.fromEpochMillis(ts + 2000));
    leftRow2.setField(2, StringData.fromString("leftTag2"));
    final var leftRow3 = new GenericRowData(3);
    leftRow3.setField(0, StringData.fromString("name1"));
    leftRow3.setField(1, TimestampData.fromEpochMillis(ts + 3000));
    leftRow3.setField(2, StringData.fromString("leftTag3"));

    final TypeInformation<RowData> leftType =
        InternalTypeInfo.of(
            new RowType(
                Arrays.asList(
                    new RowType.RowField("name", new VarCharType()),
                    new RowType.RowField("ts", new TimestampType(3)),
                    new RowType.RowField("tag", new VarCharType()))));

    final var leftSource =
        env.fromCollection(Arrays.<RowData>asList(leftRow1, leftRow2, leftRow3))
            .assignTimestampsAndWatermarks(TimestampWatermarkGenerator.create(1))
            .returns(leftType);

    final var rightRow1 = new GenericRowData(3);
    rightRow1.setField(0, StringData.fromString("name1"));
    rightRow1.setField(1, TimestampData.fromEpochMillis(ts));
    rightRow1.setField(2, StringData.fromString("rightTag1"));

    final var rightRow2 = new GenericRowData(3);
    rightRow2.setField(0, StringData.fromString("name1"));
    rightRow2.setField(1, TimestampData.fromEpochMillis(ts + 2000));
    rightRow2.setField(2, StringData.fromString("rightTag2"));

    final TypeInformation<RowData> rightType =
        InternalTypeInfo.of(
            new RowType(
                Arrays.asList(
                    new RowType.RowField("name", new VarCharType()),
                    new RowType.RowField("ts", new TimestampType(3)),
                    new RowType.RowField("tag", new VarCharType()))));

    final var rightSource =
        env.fromCollection(Arrays.<RowData>asList(rightRow1, rightRow2))
            .assignTimestampsAndWatermarks(TimestampWatermarkGenerator.create(1))
            .returns(rightType);

    final var keySelect =
        new KeySelector<RowData, StringData>() {
          @Override
          public StringData getKey(RowData rowData) {
            return rowData.getString(0);
          }
        };

    final var configuration = new Configuration();
    configuration.set(OPTION_LEFT_EVENTTIME, "ts");
    configuration.set(OPTION_RIGHT_EVENTTIME, "ts");
    configuration.set(OPTION_JOIN_TYPE, JoinType.INNER);
    configuration.set(OPTION_LEFT_SELECT_FIELDS, "*");
    configuration.set(OPTION_RIGHT_SELECT_FIELDS, "tag AS right_tag");
    configuration.set(FunctionFactory.OPTION_FUNCTION_IDENTITY, IDENTIFY);
    configuration.set(ExecutionConfigOptions.IDLE_STATE_RETENTION, Duration.ofHours(1));
    final var context = createContext(configuration);

    final var factory =
        TransformFactory.findFactory(TwoTransformFactory.IDENTITY).cast(TwoTransformFactory.class);

    final var resultStream =
        factory.transform(context, leftSource.keyBy(keySelect), rightSource.keyBy(keySelect));

    final var rowType =
        (RowType) ((DataTypeQueryable) resultStream.getType()).getDataType().getLogicalType();
    Assert.assertEquals(rowType.getFieldNames().get(0), "name");
    Assert.assertEquals(rowType.getFieldNames().get(1), "ts");
    Assert.assertEquals(rowType.getFieldNames().get(2), "tag");
    Assert.assertEquals(rowType.getFieldNames().get(3), "right_tag");

    TransformFactoryTestBase.execAndAssert(
        resultStream,
        data -> {
          Assert.assertEquals(3, data.size());
          data.sort(
              Comparator.comparingLong(o -> ((RowData) o).getTimestamp(1, 3).getMillisecond()));
          for (int i = 0; i < data.size(); i++) {
            final var rowData = (RowData) data.get(i);
            Assert.assertEquals(rowData.getString(0).toString(), "name1");
            Assert.assertEquals(rowData.getTimestamp(1, 3).getMillisecond(), ts + 1000L * (i + 1));
            Assert.assertEquals(rowData.getString(2).toString(), "leftTag" + (i + 1));
            if (i == 0) {
              Assert.assertEquals(rowData.getString(3).toString(), "rightTag1");
            } else {
              Assert.assertEquals(rowData.getString(3).toString(), "rightTag2");
            }
          }
        });
  }
}
