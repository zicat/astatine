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

/** Long2BytesAggregationFunction. */
public class Long2BytesAggregationFunction extends BytesAggregationFunction {

  @Override
  public byte[] accumulate(byte[] acc, Object value) {
    final var longValue = getLongValue(value);
    var resultBytes = initIfNull(acc);
    final var valueLength = valueLength(); // long is 8 bytes
    final var offset = nextWritableOffset(resultBytes);
    final var limit = valueLength + offset;
    // expand the byte array if necessary
    while (limit > resultBytes.length) {
      resultBytes = expand(resultBytes, offset, valueLength);
    }
    resultBytes[offset] = (byte) (longValue >>> 56);
    resultBytes[offset + 1] = (byte) (longValue >>> 48);
    resultBytes[offset + 2] = (byte) (longValue >>> 40);
    resultBytes[offset + 3] = (byte) (longValue >>> 32);
    resultBytes[offset + 4] = (byte) (longValue >>> 24);
    resultBytes[offset + 5] = (byte) (longValue >>> 16);
    resultBytes[offset + 6] = (byte) (longValue >>> 8);
    resultBytes[offset + 7] = (byte) (longValue);
    updateHead(resultBytes, limit);
    return resultBytes;
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
        return buffer.getLong();
      }
    };
  }

  protected long getLongValue(Object value) {
    if (value == null) {
      return 0L;
    }
    return (long) value;
  }

  protected int valueLength() {
    return 8;
  }
}
