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

import java.util.Arrays;

import name.zicat.astatine.streaming.sql.runtime.process.windows.Short2BytesAggregationFunction;
import org.junit.Assert;
import org.junit.Test;

public class Short2BytesAggregationFunctionTest extends BytesAggregationFunctionTestBase {

  @Test
  public void test() {
    final var function = new Short2BytesAggregationFunction();
    final var it = createIterator(function, null, Short.valueOf("4"), Short.valueOf("5"), null);
    final var expectIt =
        Arrays.asList(
                Short.valueOf("0"), Short.valueOf("4"), Short.valueOf("5"), Short.valueOf("0"))
            .iterator();
    while (it.hasNext()) {
      Assert.assertEquals(expectIt.next(), it.next());
    }
    Assert.assertFalse(expectIt.hasNext());
  }
}
