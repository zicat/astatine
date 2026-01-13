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

package name.zicat.astatine.streaming.sql.runtime.process.windows;

import java.nio.ByteBuffer;
import java.util.Iterator;

/** Boolean2BytesAggregationFunction. */
public class Boolean2BytesAggregationFunction extends Byte2BytesAggregationFunction {

  @Override
  protected byte getByteValue(Object value) {
    return (byte) ((boolean) value ? 1 : 0);
  }

  @Override
  public Iterator<Object> outputIterator(byte[] acc) {
    if (acc == null || acc.length == 0) {
      return EMPTY_ITERATOR;
    }
    final var buffer = ByteBuffer.wrap(acc);
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return buffer.hasRemaining();
      }

      @Override
      public Object next() {
        return buffer.get() == 1;
      }
    };
  }
}
