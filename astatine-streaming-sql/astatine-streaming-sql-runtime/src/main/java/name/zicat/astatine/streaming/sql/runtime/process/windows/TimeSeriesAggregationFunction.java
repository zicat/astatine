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

/** TimeSeriesAggregationFunction. */
public class TimeSeriesAggregationFunction extends Long2BytesAggregationFunction {

  public Long firstValue(byte[] value) {
    final int offset = BytesAggregationFunction.HEAD_SIZE;
    return ((long) (value[offset] & 0xFF) << 56)
        | ((long) (value[offset + 1] & 0xFF) << 48)
        | ((long) (value[offset + 2] & 0xFF) << 40)
        | ((long) (value[offset + 3] & 0xFF) << 32)
        | ((long) (value[offset + 4] & 0xFF) << 24)
        | ((long) (value[offset + 5] & 0xFF) << 16)
        | ((long) (value[offset + 6] & 0xFF) << 8)
        | ((long) (value[offset + 7] & 0xFF));
  }

  @Override
  public byte[] output(byte[] acc) {
    final var size = bodySize(acc);
    if (size == 0) {
      return acc;
    }
    /*
     The first value in acc is the start ts of session, remove it.
    */
    final var realSize = size - valueLength();
    final byte[] result = new byte[realSize];
    System.arraycopy(acc, BytesAggregationFunction.HEAD_SIZE + valueLength(), result, 0, realSize);
    return result;
  }

  @Override
  public int valueSize(byte[] acc) {
    final var size = bodySize(acc);
    if (size == 0) {
      return 0;
    }
    return size - valueLength();
  }
}
