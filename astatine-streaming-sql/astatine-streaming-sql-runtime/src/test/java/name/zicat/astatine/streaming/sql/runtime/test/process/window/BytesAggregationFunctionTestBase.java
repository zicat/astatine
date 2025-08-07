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

import name.zicat.astatine.streaming.sql.runtime.process.windows.BytesAggregationFunction;

import java.util.Iterator;

/** BytesAggregationFunctionTestBase. */
public class BytesAggregationFunctionTestBase {

  /**
   * Create an iterator for the given BytesAggregationFunction and values.
   *
   * @param function function
   * @param values values
   * @return Iterator of values
   */
  public Iterator<Object> createIterator(BytesAggregationFunction function, Object... values) {
    byte[] acc = null;
    for (Object value : values) {
      acc = function.accumulate(acc, value);
    }
    return function.outputIterator(function.output(acc));
  }
}
