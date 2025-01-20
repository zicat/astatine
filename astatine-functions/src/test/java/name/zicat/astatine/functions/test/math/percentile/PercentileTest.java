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

package name.zicat.astatine.functions.test.math.percentile;

import name.zicat.astatine.functions.statistics.percentile.Percentile;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.functions.FunctionContext;
import org.junit.Assert;
import org.junit.Test;

/** PercentileTest. */
public class PercentileTest {
  @Test
  public void test() {
    final var configuration = new Configuration();

    final var percentile = new Percentile();
    percentile.open(new FunctionContext(null, null, configuration));
    final var acc = percentile.createAccumulator();
    percentile.accumulate(acc, 1.0, 0.9);
    percentile.accumulate(acc, 0.9, 0.9);
    percentile.accumulate(acc, 0.3, 0.9);
    percentile.accumulate(acc, 0.3, 0.9);
    percentile.accumulate(acc, 0.1, 0.9);

    Assert.assertEquals(1, percentile.getValue(acc), 0.0001);
  }
}
