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

package name.zicat.astatine.format.protobuf;

import name.zicat.astatine.format.protobuf.parser.GenericArrayData2;
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.HashMap;

/** PBUtils. */
public class PBUtils {

  /**
   * default value .
   *
   * @param rowType rowType
   * @return object
   */
  public static Object[] defaultValue(RowType rowType) {
    final var defaultValue = new Object[rowType.getFieldCount()];
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      defaultValue[i] = defaultValue(rowType.getTypeAt(i));
    }
    return defaultValue;
  }

  /**
   * get default value by logic type.
   *
   * @param logicalType logicalType
   * @return object
   */
  public static Object defaultValue(LogicalType logicalType) {
    final var rootType = logicalType.getTypeRoot();
    return switch (rootType) {
      case CHAR -> StringData.fromBytes(new byte[((CharType) logicalType).getLength()]);
      case VARCHAR -> BinaryStringData.EMPTY_UTF8;
      case BOOLEAN -> false;
      case BINARY, VARBINARY -> new byte[0];
      case TINYINT -> (byte) 0;
      case SMALLINT -> (short) 0;
      case INTEGER, TIME_WITHOUT_TIME_ZONE, DATE -> 0;
      case BIGINT -> 0L;
      case FLOAT -> 0f;
      case DOUBLE -> 0d;
      case ARRAY -> new GenericArrayData2();
      case MAP -> new GenericMapData(new HashMap<>());
      case ROW -> {
        final RowType rowType = (RowType) logicalType;
        final var row = new GenericRowData(rowType.getFieldCount());
        final var values = defaultValue(rowType);
        for (int i = 0; i < values.length; i++) {
          row.setField(i, values[i]);
        }
        yield row;
      }
      case DECIMAL ->
          DecimalData.zero(
              ((DecimalType) logicalType).getPrecision(), ((DecimalType) logicalType).getScale());
      case TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE ->
          TimestampData.fromEpochMillis(0);
      case MULTISET,
          TIMESTAMP_WITH_TIME_ZONE,
          INTERVAL_YEAR_MONTH,
          INTERVAL_DAY_TIME,
          DISTINCT_TYPE,
          STRUCTURED_TYPE,
          NULL,
          RAW,
          SYMBOL,
          UNRESOLVED ->
          throw new IllegalStateException("unsupported type for default value " + rootType);
    };
  }
}
