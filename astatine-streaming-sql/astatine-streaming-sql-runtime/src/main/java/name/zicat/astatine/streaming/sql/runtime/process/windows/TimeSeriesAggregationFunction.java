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

import static name.zicat.astatine.streaming.sql.runtime.utils.VLongUtils.*;

/** TimeSeriesAggregationFunction. */
public class TimeSeriesAggregationFunction extends PositiveLong2BytesAggregationFunction<Long> {

  @Override
  protected long getLongValue(Long value) {
    return value;
  }

  public Long firstValue(byte[] value) {
    return vLongDecode(value, 4);
  }

  @Override
  public byte[] output(byte[] acc) {
    final var size = wholeSize(acc);
    if (size == 0) {
      return acc;
    }
    final var firstValueSize = firstValueSize(acc, 4);
    final var realSize = size - firstValueSize;
    final byte[] result = new byte[realSize];
    System.arraycopy(acc, 4 + firstValueSize, result, 0, realSize);
    return result;
  }

  @Override
  public int valueSize(byte[] acc) {
    final var size = wholeSize(acc);
    if (size == 0) {
      return 0;
    }
    final var firstValueSize = firstValueSize(acc, 4);
    return size - firstValueSize;
  }

  public static int firstValueSize(byte[] bytes, int index) {
    for (int i = index; i < bytes.length; i++) {
      if ((bytes[i] & 0x80) == 0) {
        return i - index + 1;
      }
    }
    throw new RuntimeException("invalid bytes");
  }
}
