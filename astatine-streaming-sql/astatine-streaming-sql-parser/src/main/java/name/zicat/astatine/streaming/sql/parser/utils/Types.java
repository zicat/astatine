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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataTypeQueryable;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** LogicalTypes. */
public class Types {

    public static int[] fieldsIndex(RowType rowType, List<String> fields) {
        if (fields == null) {
            final int[] result = new int[rowType.getFieldCount()];
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                result[i] = i;
            }
            return result;
        }

        final int[] result = new int[fields.size()];
        for (var i = 0; i < fields.size(); i++) {
            final var field = fields.get(i);
            final var index = rowType.getFieldIndex(field);
            if (index == -1) {
                throw new IllegalStateException("field not found: " + field);
            }
            result[i] = index;
        }
        return result;
    }

    public static int[] fieldsIndex(RowType rowType, String selectFieldStr) {
        final var selectFields =
                Arrays.stream(selectFieldStr.split(",")).map(String::trim).distinct().toList();
        final var hasAll = selectFields.stream().anyMatch(f -> f.equals("*"));
        return Types.fieldsIndex(rowType, hasAll ? null : selectFields);
    }

    public static int[] fieldsIndex(RowType rowType) {
        return fieldsIndex(rowType, (List<String>) null);
    }

    public static List<RowType.RowField> rowFields(RowType rowType, int[] indexes) {
        final var fields = new ArrayList<RowType.RowField>();
        for (var index : indexes) {
            final var name = rowType.getFieldNames().get(index);
            fields.add(new RowType.RowField(name, rowType.getTypeAt(index)));
        }
        return fields;
    }

    public static List<RowType.RowField> rowFieldsNullable(RowType rowType, int[] indexes) {
        final var fields = new ArrayList<RowType.RowField>();
        for (var index : indexes) {
            final var name = rowType.getFieldNames().get(index);
            fields.add(new RowType.RowField(name, rowType.getTypeAt(index).copy(true)));
        }
        return fields;
    }

    public static List<RowData.FieldGetter> fieldGetters(RowType rowType, int... indexes) {
        final var fieldGetters = new ArrayList<RowData.FieldGetter>();
        for (var index : indexes) {
            fieldGetters.add(RowData.createFieldGetter(rowType.getTypeAt(index), index));
        }
        return fieldGetters;
    }

    public static RowData.FieldGetter fieldGetter(RowType rowType, String fieldName) {
        final var index = rowType.getFieldIndex(fieldName);
        return RowData.createFieldGetter(rowType.getTypeAt(index), index);
    }

    public static int fieldIndex(DataTypeQueryable typeInfo, String name) {
        final var index = ((RowType) typeInfo.getDataType().getLogicalType()).getFieldIndex(name);
        if (index == -1) {
            throw new IllegalStateException("field not found: " + name);
        }
        return index;
    }
}
