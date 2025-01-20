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

package name.zicat.astatine.functions.test.math.hyperloglog;

import name.zicat.astatine.functions.statistics.hyperloglog.AbstractHyperloglog;
import name.zicat.astatine.functions.statistics.hyperloglog.Hyperloglog;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.functions.FunctionContext;
import org.junit.Assert;
import org.junit.Test;

/** HyperloglogTest. */
public class HyperloglogTest {

  @Test
  public void test() {

    final var configuration = new Configuration();
    configuration.setInteger(AbstractHyperloglog.NUMBER_OF_SLOT, 1024);
    configuration.setInteger(AbstractHyperloglog.NUMBER_OF_PER_SLOT, 4);

    final var hyperloglog = new Hyperloglog();
    hyperloglog.open(new FunctionContext(null, null, configuration));
    final var acc = hyperloglog.createAccumulator();
    hyperloglog.accumulate(acc, 1L);
    hyperloglog.accumulate(acc, 2L);
    hyperloglog.accumulate(acc, 3L);
    hyperloglog.accumulate(acc, 2L);
    hyperloglog.accumulate(acc, 3L);
    Assert.assertEquals(3L, hyperloglog.getValue(acc).longValue());
  }
}
