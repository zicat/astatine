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

import name.zicat.astatine.streaming.sql.runtime.process.windows.*;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.junit.Assert;
import org.junit.Test;

/** BytesAggregationFunctionTest. */
public class BytesAggregationFunctionTest {

  @Test
  public void testCreateAggregationFunction() {
    Assert.assertEquals(
        StringData2BytesAggregationFunction.class,
        BytesAggregationFunction.createAggregationFunction(LogicalTypeRoot.CHAR).getClass());
    Assert.assertEquals(
        StringData2BytesAggregationFunction.class,
        BytesAggregationFunction.createAggregationFunction(LogicalTypeRoot.VARCHAR).getClass());

    Assert.assertEquals(
        Binary2BytesAggregationFunction.class,
        BytesAggregationFunction.createAggregationFunction(LogicalTypeRoot.VARBINARY).getClass());
    Assert.assertEquals(
        Binary2BytesAggregationFunction.class,
        BytesAggregationFunction.createAggregationFunction(LogicalTypeRoot.BINARY).getClass());

    Assert.assertEquals(
        Byte2BytesAggregationFunction.class,
        BytesAggregationFunction.createAggregationFunction(LogicalTypeRoot.TINYINT).getClass());

    Assert.assertEquals(
        Short2BytesAggregationFunction.class,
        BytesAggregationFunction.createAggregationFunction(LogicalTypeRoot.SMALLINT).getClass());

    Assert.assertEquals(
        Int2BytesAggregationFunction.class,
        BytesAggregationFunction.createAggregationFunction(LogicalTypeRoot.INTEGER).getClass());
    Assert.assertEquals(
        Int2BytesAggregationFunction.class,
        BytesAggregationFunction.createAggregationFunction(LogicalTypeRoot.DATE).getClass());
    Assert.assertEquals(
        Int2BytesAggregationFunction.class,
        BytesAggregationFunction.createAggregationFunction(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE)
            .getClass());
    Assert.assertEquals(
        Int2BytesAggregationFunction.class,
        BytesAggregationFunction.createAggregationFunction(LogicalTypeRoot.INTERVAL_DAY_TIME)
            .getClass());
    Assert.assertEquals(
        Int2BytesAggregationFunction.class,
        BytesAggregationFunction.createAggregationFunction(LogicalTypeRoot.INTERVAL_YEAR_MONTH)
            .getClass());

    Assert.assertEquals(
        Long2BytesAggregationFunction.class,
        BytesAggregationFunction.createAggregationFunction(LogicalTypeRoot.BIGINT).getClass());

    Assert.assertEquals(
        Float2BytesAggregationFunction.class,
        BytesAggregationFunction.createAggregationFunction(LogicalTypeRoot.FLOAT).getClass());

    Assert.assertEquals(
        Double2BytesAggregationFunction.class,
        BytesAggregationFunction.createAggregationFunction(LogicalTypeRoot.DOUBLE).getClass());

    try {
      BytesAggregationFunction.createAggregationFunction(LogicalTypeRoot.UNRESOLVED);
      Assert.fail();
    } catch (Exception ignore) {
    }
  }
}
