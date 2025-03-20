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

/** PositiveLong2BytesAggregationFunction. */
public abstract class PositiveLong2BytesAggregationFunction<T>
    implements AggregationFunction<T, byte[]> {

  private static final int DEFAULT_SIZE = 64;

  @Override
  public byte[] accumulate(byte[] acc, T value) {
    var longValue = value == null ? 0L: getLongValue(value);
    if (longValue < 0) {
      throw new IllegalArgumentException("long value must be positive");
    }
    var resultBytes = acc == null || acc.length < 4 ? new byte[DEFAULT_SIZE] : acc;
    var size = wholeSize(acc);
    final var length = calculateVLongLength(longValue);
    final var offset = 4 + size;
    // expand the byte array if necessary
    if (offset + length > resultBytes.length) {
      var newBytes = new byte[resultBytes.length + DEFAULT_SIZE];
      System.arraycopy(resultBytes, 0, newBytes, 0, offset);
      resultBytes = newBytes;
    }
    for (int i = 0; i < length; i++) {
      resultBytes[offset + i] = (byte) ((longValue & 0x7F) | (i < length - 1 ? 0x80 : 0));
      longValue >>>= 7;
    }
    final var newSize = size + length;
    resultBytes[0] = (byte) ((newSize >>> 24) & 0xFF);
    resultBytes[1] = (byte) ((newSize >>> 16) & 0xFF);
    resultBytes[2] = (byte) ((newSize >>> 8) & 0xFF);
    resultBytes[3] = (byte) (newSize & 0xFF);
    return resultBytes;
  }

  @Override
  public byte[] output(byte[] acc) {
    var size = wholeSize(acc);
    if (size == 0) {
      return acc;
    }
    final byte[] result = new byte[size];
    System.arraycopy(acc, 4, result, 0, size);
    return result;
  }

  @Override
  public int valueSize(byte[] acc) {
    return wholeSize(acc);
  }

  protected int wholeSize(byte[] acc) {
    if (acc == null || acc.length < 4) {
      return 0;
    }
    return ((acc[0] & 0xFF) << 24)
        | ((acc[1] & 0xFF) << 16)
        | ((acc[2] & 0xFF) << 8)
        | (acc[3] & 0xFF);
  }

  /** get long value. */
  protected abstract long getLongValue(T value);
}
