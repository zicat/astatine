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

import org.apache.flink.table.data.StringData;

import java.nio.ByteBuffer;
import java.util.Iterator;

import static name.zicat.astatine.streaming.sql.runtime.utils.VLongUtils.vLongDecode;

/** StringDataAggregationFunction. */
public class StringData2BytesAggregationFunction extends Binary2BytesAggregationFunction {

  @Override
  protected byte[] getBinary(Object value) {
    final var stringData = (StringData) value;
    return stringData.toBytes();
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
        final var length = (int) vLongDecode(buffer);
        final var value = new byte[length];
        buffer.get(value);
        return StringData.fromBytes(value);
      }
    };
  }
}
