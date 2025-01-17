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

import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.types.RowKind;

import java.util.Arrays;

/** ModifiableProjectRowData. */
public class ModifiableProjectRowData implements RowData {
    private final int[] indexMapping;
    private RowData row;

    private ModifiableProjectRowData(int[] indexMapping) {
        this.indexMapping = indexMapping;
    }

    public ModifiableProjectRowData replaceRow(RowData row) {
        this.row = row;
        return this;
    }

    public int getArity() {
        return this.indexMapping.length;
    }

    public RowKind getRowKind() {
        return this.row.getRowKind();
    }

    public void setRowKind(RowKind kind) {
        this.row.setRowKind(kind);
    }

    public boolean isNullAt(int pos) {
        return this.row.isNullAt(this.indexMapping[pos]);
    }

    public boolean getBoolean(int pos) {
        return this.row.getBoolean(this.indexMapping[pos]);
    }

    public byte getByte(int pos) {
        return this.row.getByte(this.indexMapping[pos]);
    }

    public short getShort(int pos) {
        return this.row.getShort(this.indexMapping[pos]);
    }

    public int getInt(int pos) {
        return this.row.getInt(this.indexMapping[pos]);
    }

    public long getLong(int pos) {
        return this.row.getLong(this.indexMapping[pos]);
    }

    public float getFloat(int pos) {
        return this.row.getFloat(this.indexMapping[pos]);
    }

    public double getDouble(int pos) {
        return this.row.getDouble(this.indexMapping[pos]);
    }

    public StringData getString(int pos) {
        return this.row.getString(this.indexMapping[pos]);
    }

    public DecimalData getDecimal(int pos, int precision, int scale) {
        return this.row.getDecimal(this.indexMapping[pos], precision, scale);
    }

    public TimestampData getTimestamp(int pos, int precision) {
        return this.row.getTimestamp(this.indexMapping[pos], precision);
    }

    public <T> RawValueData<T> getRawValue(int pos) {
        return this.row.getRawValue(this.indexMapping[pos]);
    }

    public byte[] getBinary(int pos) {
        return this.row.getBinary(this.indexMapping[pos]);
    }

    public ArrayData getArray(int pos) {
        return this.row.getArray(this.indexMapping[pos]);
    }

    public MapData getMap(int pos) {
        return this.row.getMap(this.indexMapping[pos]);
    }

    public RowData getRow(int pos, int numFields) {
        return this.row.getRow(this.indexMapping[pos], numFields);
    }

    public void setInt(int pos, int value) {
        if (this.row instanceof GenericRowData genericRowData) {
            genericRowData.setField(this.indexMapping[pos], value);
        } else if (this.row instanceof BinaryRowData binaryRowData) {
            binaryRowData.setInt(this.indexMapping[pos], value);
        } else {
            throw new RuntimeException("not support to modify row");
        }
    }

    public boolean equals(Object o) {
        throw new UnsupportedOperationException("Projected row data cannot be compared");
    }

    public int hashCode() {
        throw new UnsupportedOperationException("Projected row data cannot be hashed");
    }

    public String toString() {
        String var10000 = this.row.getRowKind().shortString();
        return var10000
                + "{indexMapping="
                + Arrays.toString(this.indexMapping)
                + ", mutableRow="
                + this.row
                + "}";
    }

    public static ModifiableProjectRowData from(int[][] projection)
            throws IllegalArgumentException {
        return new ModifiableProjectRowData(
                Arrays.stream(projection)
                        .mapToInt(
                                (arr) -> {
                                    if (arr.length != 1) {
                                        throw new IllegalArgumentException(
                                                "ProjectedRowData doesn't support nested projections");
                                    } else {
                                        return arr[0];
                                    }
                                })
                        .toArray());
    }

    public static ModifiableProjectRowData from(int[] projection) {
        return new ModifiableProjectRowData(projection);
    }

    public static ModifiableProjectRowData from(Projection projection) {
        return new ModifiableProjectRowData(projection.toTopLevelIndexes());
    }
}
