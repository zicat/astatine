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

import static name.zicat.astatine.streaming.sql.runtime.utils.VLongUtils.calculateVLongLength;

/** BinaryAggregationFunction. */
public class BinaryAggregationFunction extends BytesAggregationFunction {

  protected static final byte[] EMPTY = new byte[0];

  @Override
  public byte[] accumulate(byte[] acc, Object originalValue) {
    var resultBytes = initIfNull(acc);
    final var valueBytes = getBinary(originalValue);
    final var vlongLength = calculateVLongLength(valueBytes.length);
    final var valueLength = valueBytes.length + vlongLength;
    final var offset = nextWritableOffset(resultBytes);
    final var limit = valueLength + offset;
    // expand the byte array if necessary
    while (limit > resultBytes.length) {
      resultBytes = expand(resultBytes, offset, valueLength);
    }

    var longValue = valueBytes.length;
    for (int i = 0; i < vlongLength; i++) {
      resultBytes[offset + i] = (byte) ((longValue & 0x7F));
      longValue >>>= 7;
    }
    System.arraycopy(valueBytes, 0, resultBytes, offset + vlongLength, valueBytes.length);
    updateHead(resultBytes, limit);
    return resultBytes;
  }

  protected byte[] getBinary(Object value) {
    if (value == null) {
      return EMPTY;
    }
    return (byte[]) value;
  }
}
