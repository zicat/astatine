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

/** Int2BytesAggregationFunction. */
public class Int2BytesAggregationFunction extends BytesAggregationFunction {

  @Override
  public byte[] accumulate(byte[] acc, Object value) {
    final var intValue = getIntValue(value);
    var resultBytes = initIfNull(acc);
    final var valueLength = 4; // int is 4 bytes
    final var offset = nextWritableOffset(resultBytes);
    final var limit = valueLength + offset;
    // expand the byte array if necessary
    while (limit > resultBytes.length) {
      resultBytes = expand(resultBytes, offset, valueLength);
    }
    resultBytes[offset] = (byte) (intValue >>> 24);
    resultBytes[offset + 1] = (byte) (intValue >>> 16);
    resultBytes[offset + 2] = (byte) (intValue >>> 8);
    resultBytes[offset + 3] = (byte) (intValue);
    updateHead(resultBytes, limit);
    return resultBytes;
  }

  protected int getIntValue(Object value) {
    if (value == null) {
      return 0;
    }
    return (int) value;
  }
}
