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

package name.zicat.astatine.streaming.sql.runtime.utils;

/** VLongUtils. */
public class VLongUtils {

  public static int calculateVLongLength(long value) {
    if (value == 0) {
      return 1;
    }
    int bits = Long.SIZE - Long.numberOfLeadingZeros(value);
    return (bits + 6) / 7;
  }

  public static long vLongDecode(byte[] bytes, int index) {
    long value = 0;
    int shift = 0;
    for (int i = index; i < bytes.length; i++) {
      value |= (long) (bytes[i] & 0x7F) << shift;
      if ((bytes[i] & 0x80) == 0) {
        break;
      }
      shift += 7;
    }
    return value;
  }

  public static long zigZagEncode(int n) {
    return ((long) n << 1) ^ (n >> 31);
  }

  public static int zigZagDecode(long n) {
    return (int) ((n >>> 1) ^ -(n & 1));
  }
}
