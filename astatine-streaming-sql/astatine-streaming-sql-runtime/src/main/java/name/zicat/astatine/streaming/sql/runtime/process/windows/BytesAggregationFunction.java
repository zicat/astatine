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

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.Iterator;

/** BytesAggregationFunction. */
public abstract class BytesAggregationFunction implements AggregationFunction<byte[]> {

  protected static final Iterator<Object> EMPTY_ITERATOR =
      new Iterator<>() {
        @Override
        public boolean hasNext() {
          return false;
        }

        @Override
        public Object next() {
          return null;
        }
      };
  protected static final int HEAD_SIZE = 4;
  protected static final int DEFAULT_SIZE = 64;

  @Override
  public byte[] output(byte[] acc) {
    var size = bodySize(acc);
    if (size == 0) {
      return acc;
    }
    // remove head
    final byte[] result = new byte[size];
    System.arraycopy(acc, HEAD_SIZE, result, 0, size);
    return result;
  }

  /**
   * output the iterator of values.
   *
   * @param acc acc
   * @return Iterator of values
   */
  public abstract Iterator<Object> outputIterator(byte[] acc);

  @Override
  public int valueSize(byte[] acc) {
    return bodySize(acc);
  }

  protected int bodySize(byte[] acc) {
    if (acc == null || acc.length < 4) {
      return 0;
    }
    return ((acc[0] & 0xFF) << 24)
        | ((acc[1] & 0xFF) << 16)
        | ((acc[2] & 0xFF) << 8)
        | (acc[3] & 0xFF);
  }

  protected int nextWritableOffset(byte[] acc) {
    return acc == null || acc.length < HEAD_SIZE ? HEAD_SIZE : bodySize(acc) + HEAD_SIZE;
  }

  protected byte[] initIfNull(byte[] acc) {
    return acc == null || acc.length < HEAD_SIZE ? new byte[DEFAULT_SIZE] : acc;
  }

  protected byte[] expand(byte[] acc, int copySize, int minSize) {
    var newBytes = new byte[acc.length + Math.max(minSize, DEFAULT_SIZE)];
    System.arraycopy(acc, 0, newBytes, 0, copySize);
    return newBytes;
  }

  protected void updateHead(byte[] acc, int limit) {
    final var size = limit - HEAD_SIZE;
    acc[0] = (byte) ((size >>> 24) & 0xFF);
    acc[1] = (byte) ((size >>> 16) & 0xFF);
    acc[2] = (byte) ((size >>> 8) & 0xFF);
    acc[3] = (byte) (size & 0xFF);
  }

  /**
   * create aggregation function.
   *
   * @param fieldType fieldType
   * @return AggregationFunction
   */
  public static BytesAggregationFunction createAggregationFunction(LogicalType fieldType) {
    return createAggregationFunction(fieldType.getTypeRoot());
  }

  /**
   * create aggregation function.
   *
   * @param rootType rootType
   * @return AggregationFunction
   */
  public static BytesAggregationFunction createAggregationFunction(LogicalTypeRoot rootType) {
    return switch (rootType) {
      case CHAR, VARCHAR -> new StringData2BytesAggregationFunction();
      case BOOLEAN -> new Boolean2BytesAggregationFunction();
      case BINARY, VARBINARY -> new Binary2BytesAggregationFunction();
      case TINYINT -> new Byte2BytesAggregationFunction();
      case SMALLINT -> new Short2BytesAggregationFunction();
      case INTEGER, DATE, TIME_WITHOUT_TIME_ZONE, INTERVAL_YEAR_MONTH, INTERVAL_DAY_TIME ->
          new Int2BytesAggregationFunction();
      case BIGINT -> new Long2BytesAggregationFunction();
      case FLOAT -> new Float2BytesAggregationFunction();
      case DOUBLE -> new Double2BytesAggregationFunction();
      default -> throw new IllegalArgumentException();
    };
  }
}
