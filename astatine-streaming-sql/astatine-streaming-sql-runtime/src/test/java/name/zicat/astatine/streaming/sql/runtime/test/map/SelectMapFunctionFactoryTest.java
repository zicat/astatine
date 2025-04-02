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

import name.zicat.astatine.streaming.sql.parser.function.FunctionFactory;
import name.zicat.astatine.streaming.sql.parser.test.transform.TransformFactoryTestBase;
import name.zicat.astatine.streaming.sql.parser.transform.MapTransformFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformFactory;
import name.zicat.astatine.streaming.sql.runtime.map.SelectMapFunctionFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/** SelectMapFunctionFactoryTest. */
public class SelectMapFunctionFactoryTest extends TransformFactoryTestBase {

  @Test
  public void testSessionEndAndCreateNew() throws Exception {
    final var configuration = new Configuration();
    configuration.set(FunctionFactory.OPTION_FUNCTION_IDENTITY, SelectMapFunctionFactory.IDENTITY);
    configuration.set(
        SelectMapFunctionFactory.OPTION_EXPRESSION, "UPPER(name) AS n1, LOWER(name) AS n2");
    final var context = createContext(configuration);
    final var factory =
        TransformFactory.findFactory(MapTransformFactory.IDENTITY).cast(MapTransformFactory.class);
    final var row1 = GenericRowData.of(StringData.fromString("NsN"));

    final var source =
        env.fromCollection(List.<RowData>of(row1))
            .returns(
                InternalTypeInfo.of(
                    new RowType(List.of(new RowType.RowField("name", new VarCharType())))));

    final var result = factory.transform(context, source);
    TransformFactoryTestBase.execAndAssert(
        result,
        data -> {
          Assert.assertEquals(1, data.size());
          final var resultRow0 = (RowData) data.get(0);
          Assert.assertEquals("NSN", resultRow0.getString(0).toString());
          Assert.assertEquals("nsn", resultRow0.getString(1).toString());
        });
  }
}
