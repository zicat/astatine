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

package name.zicat.astatine.streaming.sql.runtime.test.map;

import name.zicat.astatine.streaming.sql.parser.test.transform.TransformFactoryTestBase;
import name.zicat.astatine.streaming.sql.parser.transform.FlatMapTransformFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformFactory;
import name.zicat.astatine.streaming.sql.runtime.map.DateExpansionFlatMapFunctionFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.*;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static name.zicat.astatine.streaming.sql.parser.function.FunctionFactory.OPTION_FUNCTION_IDENTITY;
import static name.zicat.astatine.streaming.sql.runtime.map.DateExpansionFlatMapFunctionFactory.*;

/** DateExpansionFlatMapFunctionFactoryTest. */
public class DateExpansionFlatMapFunctionFactoryTest extends TransformFactoryTestBase {

  @Test
  public void testCrossDateIn1min() throws Exception {
    final var configuration = new Configuration();
    final var zoneId = ZoneId.of("GMT");
    final var maxPartitionCount = 4;
    configuration.set(OPTION_FUNCTION_IDENTITY, DateExpansionFlatMapFunctionFactory.IDENTITY);
    configuration.set(OPTION_TIME_ZONE, zoneId.getId());
    configuration.set(OPTION_FIELD_START_TS, "start_ts");
    configuration.set(OPTION_FIELD_END_TS, "end_ts");
    configuration.set(OPTION_FIELD_CURRENT_TS, "ts");
    configuration.set(OPTION_MAX_COUNT, maxPartitionCount);

    final var context = createContext(configuration);
    final var factory =
        TransformFactory.findFactory(FlatMapTransformFactory.IDENTITY)
            .cast(FlatMapTransformFactory.class);
    final var sourceType =
        InternalTypeInfo.of(
            new RowType(
                Arrays.asList(
                    new RowType.RowField("name", new VarCharType()),
                    new RowType.RowField("ts", new BigIntType()),
                    new RowType.RowField("start_ts", new BigIntType()),
                    new RowType.RowField("end_ts", new BigIntType()))));
    final var startTs = 1746575999000L; // 2025-05-06 23:59:59.000 UTC
    final var endTs = 1746576001000L; // 2025-05-07 00:00:01.000 UTC
    final var row1 = new GenericRowData(5);
    row1.setField(0, StringData.fromString("name1"));
    row1.setField(1, endTs);
    row1.setField(2, startTs);
    row1.setField(3, endTs);

    final var source = env.fromCollection(List.<RowData>of(row1)).returns(sourceType);
    final var result = factory.transform(context, source);
    TransformFactoryTestBase.execAndAssert(
        result,
        data -> {
          Assert.assertEquals(2, data.size());
          Assert.assertEquals(20215, ((RowData) data.get(0)).getInt(4));
          Assert.assertEquals(20214, ((RowData) data.get(1)).getInt(4));
        });
  }

  @Test
  public void test() throws Exception {

    final var configuration = new Configuration();
    final var zoneId = ZoneId.of("GMT+8");
    final var maxPartitionCount = 4;
    configuration.set(OPTION_FUNCTION_IDENTITY, DateExpansionFlatMapFunctionFactory.IDENTITY);
    configuration.set(OPTION_TIME_ZONE, zoneId.getId());
    configuration.set(OPTION_FIELD_START_TS, "start_ts");
    configuration.set(OPTION_FIELD_END_TS, "end_ts");
    configuration.set(OPTION_FIELD_CURRENT_TS, "ts");
    configuration.set(OPTION_MAX_COUNT, maxPartitionCount);

    final var context = createContext(configuration);
    final var factory =
        TransformFactory.findFactory(FlatMapTransformFactory.IDENTITY)
            .cast(FlatMapTransformFactory.class);
    final var sourceType =
        InternalTypeInfo.of(
            new RowType(
                Arrays.asList(
                    new RowType.RowField("name", new VarCharType()),
                    new RowType.RowField("ts", new IntType()),
                    new RowType.RowField("start_ts", new IntType()),
                    new RowType.RowField("end_ts", new IntType()),
                    new RowType.RowField("date", new DateType()))));
    final var ts = 1739311800;
    final int currentDate =
        (int) Instant.ofEpochSecond(ts).atZone(zoneId).toLocalDate().toEpochDay();
    final var row1 = new GenericRowData(5);
    row1.setField(0, StringData.fromString("name1"));
    row1.setField(1, ts);
    row1.setField(2, ts - 3600 * 24);
    row1.setField(3, ts + 3600 * 24);
    row1.setField(4, currentDate);

    final var row2 = new GenericRowData(5);
    row2.setField(0, StringData.fromString("name2"));
    row2.setField(1, ts);
    row2.setField(4, currentDate);

    final var row3 = new GenericRowData(5);
    row3.setField(0, StringData.fromString("name3"));
    row3.setField(1, ts);
    row3.setField(3, ts - 3600 * 24);
    row3.setField(4, currentDate);

    final var row4 = new GenericRowData(5);
    row4.setField(0, StringData.fromString("name4"));
    row4.setField(1, ts);
    row4.setField(2, ts - 3600 * 24 * 5);
    row4.setField(3, ts + 3600 * 24);
    row4.setField(4, currentDate);

    final var row5 = new GenericRowData(5);
    row5.setField(0, StringData.fromString("name5"));
    row5.setField(1, ts);
    row5.setField(2, ts - 1);
    row5.setField(3, ts + 1);
    row5.setField(4, currentDate);

    final var source =
        env.fromCollection(List.<RowData>of(row1, row2, row3, row4, row5)).returns(sourceType);
    final var result = factory.transform(context, source);

    TransformFactoryTestBase.execAndAssert(
        result,
        data -> {
          final var n1List = filterByName(data, "name1");
          Assert.assertEquals(3, n1List.size());
          for (int i = 0; i < n1List.size(); i++) {
            final var row = n1List.get(i);
            Assert.assertEquals(currentDate, row.getInt(4));
            Assert.assertEquals(currentDate - 1 + i, row.getInt(5));
          }

          // has current no start end
          final var n2List = filterByName(data, "name2");
          Assert.assertEquals(maxPartitionCount, n2List.size());
          for (int i = 0; i < n2List.size(); i++) {
            final var row = n2List.get(i);
            Assert.assertEquals(currentDate, row.getInt(4));
            Assert.assertEquals(currentDate - maxPartitionCount + i + 1, row.getInt(5));
          }

          // end is smaller than current, no start
          final var n3List = filterByName(data, "name3");
          Assert.assertEquals(maxPartitionCount, n3List.size());
          for (int i = 0; i < n3List.size(); i++) {
            final var row = n3List.get(i);
            Assert.assertEquals(currentDate, row.getInt(4));
            Assert.assertEquals(currentDate - maxPartitionCount + i, row.getInt(5));
          }

          // end - start over maxPartitionCount
          final var n4List = filterByName(data, "name4");
          Assert.assertEquals(maxPartitionCount, n4List.size());
          for (int i = 0; i < n4List.size(); i++) {
            final var row = n4List.get(i);
            Assert.assertEquals(currentDate, row.getInt(4));
            Assert.assertEquals(currentDate - maxPartitionCount + i + 2, row.getInt(5));
          }

          // end, start in same day
          final var n5List = filterByName(data, "name5");
          Assert.assertEquals(1, n5List.size());
          for (final RowData row : n5List) {
            Assert.assertEquals(currentDate, row.getInt(4));
            Assert.assertEquals(currentDate, row.getInt(5));
          }
        });
  }

  private static List<RowData> filterByName(List<?> data, String name) {
    return data.stream()
        .map(v -> (RowData) v)
        .filter(v -> v.getString(0).toString().equals(name))
        .sorted(LAST_VALUE_ASC)
        .toList();
  }

  private static final Comparator<Object> LAST_VALUE_ASC =
      (o1, o2) -> {
        final var row1 = (RowData) o1;
        final var row2 = (RowData) o2;
        return Integer.compare(row1.getInt(row1.getArity() - 1), row2.getInt(row2.getArity() - 1));
      };
}
