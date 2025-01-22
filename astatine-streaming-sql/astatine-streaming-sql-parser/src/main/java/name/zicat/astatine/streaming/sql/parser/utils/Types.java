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

package name.zicat.astatine.streaming.sql.parser.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataTypeQueryable;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.List;

/** Types. */
public class Types {

  /**
   * convert typeInformation to rowType.
   *
   * @param typeInformation typeInformation
   * @return RowType
   */
  public static RowType toRowType(TypeInformation<?> typeInformation) {
    return ((RowType) ((DataTypeQueryable) typeInformation).getDataType().getLogicalType());
  }

  /**
   * get field name type by rowType and field.
   *
   * @param rowType rowType
   * @param field field like name AS name_1 or name
   * @return FieldNameType
   */
  public static FieldNameType fieldNameType(RowType rowType, String field) {
    return Types.fieldsNameTypes(rowType, List.of(field))[0];
  }

  /**
   * convert rowType to FieldNameType array.
   *
   * @param rowType rowType
   * @return FieldNameType Array
   */
  public static FieldNameType[] fieldsNameTypes(RowType rowType) {
    final var result = new FieldNameType[rowType.getFieldCount()];
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      result[i] = new FieldNameType(i, rowType.getTypeAt(i), rowType.getFieldNames().get(i));
    }
    return result;
  }

  /**
   * get field name type by rowType and fields in rowType.
   *
   * @param rowType rowType
   * @param fields fields array, field in field like name AS name_1 or name
   * @return FieldNameType Array
   */
  public static FieldNameType[] fieldsNameTypes(RowType rowType, List<String> fields) {
    final var result = new FieldNameType[fields.size()];
    for (var i = 0; i < fields.size(); i++) {
      final var fieldSplit = fields.get(i).split(" AS ");
      final var field = fieldSplit[0].trim();
      final var targetName = fieldSplit.length == 2 ? fieldSplit[1].trim() : field;
      final var index = rowType.getFieldIndex(field);
      if (index == -1) {
        throw new IllegalStateException("field not found: " + field);
      }
      result[i] = new FieldNameType(index, rowType.getTypeAt(index), field, targetName);
    }
    return result;
  }

  /**
   * get field name type by rowType and select expression.
   *
   * @param rowType rowType
   * @param expression expression like name AS name_1, age AS age_1 or *
   * @return FieldNameType Array
   */
  public static FieldNameType[] fieldsNameTypes(RowType rowType, String expression) {
    final var selectFields =
        Arrays.stream(expression.split(",")).map(String::trim).distinct().toList();
    final var hasAll = selectFields.stream().anyMatch(f -> f.equals("*"));
    return hasAll ? fieldsNameTypes(rowType) : fieldsNameTypes(rowType, selectFields);
  }

  /** FieldNameType. */
  public static class FieldNameType {
    private final int index;
    private final LogicalType type;
    private final String sourceName;
    private final String targetName;

    public FieldNameType(int index, LogicalType type, String sourceName, String targetName) {
      this.index = index;
      this.type = type;
      this.sourceName = sourceName;
      this.targetName = targetName;
    }

    public FieldNameType(int index, LogicalType type, String name) {
      this(index, type, name, name);
    }

    public int getIndex() {
      return index;
    }

    public LogicalType getType() {
      return type;
    }

    public String getSourceName() {
      return sourceName;
    }

    public String getTargetName() {
      return targetName;
    }

    public RowType.RowField targetRowField() {
      return new RowType.RowField(targetName, type);
    }

    public RowType.RowField targetNullableRowField() {
      return new RowType.RowField(targetName, type.copy(true));
    }

    public RowData.FieldGetter fieldGetter() {
      return RowData.createFieldGetter(type, index);
    }
  }
}
