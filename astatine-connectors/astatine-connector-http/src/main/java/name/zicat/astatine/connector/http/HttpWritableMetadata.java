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

package name.zicat.astatine.connector.http;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.io.Serial;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/** HttpWritableMetadata. */
public enum HttpWritableMetadata {
  BODY(
      "body",
      DataTypes.BYTES().nullable(),
      new MetadataConverter() {
        @Serial private static final long serialVersionUID = 1L;

        @Override
        public Object read(RowData row, int pos) {
          if (row.isNullAt(pos)) {
            return null;
          }
          return row.getBinary(pos);
        }
      }),

  HEADERS(
      "headers",
      // key and value of the map are nullable to make handling easier in queries
      DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.STRING().nullable()).nullable(),
      new MetadataConverter() {
        @Serial private static final long serialVersionUID = 1L;

        @Override
        public Object read(RowData row, int pos) {
          if (row.isNullAt(pos)) {
            return null;
          }
          final MapData map = row.getMap(pos);
          final ArrayData keyArray = map.keyArray();
          final ArrayData valueArray = map.valueArray();
          final Map<String, String> result = new HashMap<>();
          for (int i = 0; i < keyArray.size(); i++) {
            if (!keyArray.isNullAt(i) && !valueArray.isNullAt(i)) {
              final String key = keyArray.getString(i).toString();
              final String value = valueArray.getString(i).toString();
              result.put(key, value);
            }
          }
          return result;
        }
      }),

  URL(
      "url",
      DataTypes.STRING().notNull(),
      new MetadataConverter() {

        @Serial private static final long serialVersionUID = 1L;

        @Override
        public Object read(RowData row, int pos) {
          if (row.isNullAt(pos)) {
            return null;
          }
          return row.getString(pos).toString();
        }
      });

  final String key;

  final DataType dataType;

  final MetadataConverter converter;

  HttpWritableMetadata(String key, DataType dataType, MetadataConverter converter) {
    this.key = key;
    this.dataType = dataType;
    this.converter = converter;
  }

  /** MetadataConverter. */
  interface MetadataConverter extends Serializable {

    /**
     * read metadata.
     *
     * @param consumedRow consumedRow
     * @param pos pos
     * @return result
     */
    Object read(RowData consumedRow, int pos);
  }
}
