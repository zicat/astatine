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

package name.zicat.astatine.format.protobuf.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

/** GenericArrayData2. */
public class GenericArrayData2 implements ArrayData {

    private final List<Object> values;

    public GenericArrayData2(List<Object> values) {
        this.values = values;
    }

    public GenericArrayData2() {
        this(new ArrayList<>());
    }

    @Override
    public int size() {
        return values.size();
    }

    @Override
    public boolean isNullAt(int pos) {
        return values.get(pos) == null;
    }

    @Override
    public boolean getBoolean(int pos) {
        return (boolean) values.get(pos);
    }

    @Override
    public byte getByte(int pos) {
        return (byte) values.get(pos);
    }

    @Override
    public short getShort(int pos) {
        return (short) values.get(pos);
    }

    @Override
    public int getInt(int pos) {
        return (int) values.get(pos);
    }

    @Override
    public long getLong(int pos) {
        return (long) values.get(pos);
    }

    @Override
    public float getFloat(int pos) {
        return (float) values.get(pos);
    }

    @Override
    public double getDouble(int pos) {
        return (double) values.get(pos);
    }

    @Override
    public StringData getString(int pos) {
        return (StringData) values.get(pos);
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        return (DecimalData) values.get(pos);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        return (TimestampData) values.get(pos);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        return (RawValueData<T>) values.get(pos);
    }

    @Override
    public byte[] getBinary(int pos) {
        return (byte[]) values.get(pos);
    }

    @Override
    public ArrayData getArray(int pos) {
        return (ArrayData) values.get(pos);
    }

    @Override
    public MapData getMap(int pos) {
        return (MapData) values.get(pos);
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        return (RowData) values.get(pos);
    }

    @Override
    public boolean[] toBooleanArray() {
        final boolean[] result = new boolean[values.size()];
        for (int i = 0; i < values.size(); i++) {
            result[i] = (boolean) values.get(i);
        }
        return result;
    }

    @Override
    public byte[] toByteArray() {
        final byte[] result = new byte[values.size()];
        for (int i = 0; i < values.size(); i++) {
            result[i] = (byte) values.get(i);
        }
        return result;
    }

    @Override
    public short[] toShortArray() {
        final short[] result = new short[values.size()];
        for (int i = 0; i < values.size(); i++) {
            result[i] = (short) values.get(i);
        }
        return result;
    }

    @Override
    public int[] toIntArray() {
        final int[] result = new int[values.size()];
        for (int i = 0; i < values.size(); i++) {
            result[i] = (int) values.get(i);
        }
        return result;
    }

    @Override
    public long[] toLongArray() {
        final long[] result = new long[values.size()];
        for (int i = 0; i < values.size(); i++) {
            result[i] = (long) values.get(i);
        }
        return result;
    }

    @Override
    public float[] toFloatArray() {
        final float[] result = new float[values.size()];
        for (int i = 0; i < values.size(); i++) {
            result[i] = (float) values.get(i);
        }
        return result;
    }

    @Override
    public double[] toDoubleArray() {
        final double[] result = new double[values.size()];
        for (int i = 0; i < values.size(); i++) {
            result[i] = (double) values.get(i);
        }
        return result;
    }

    public void add(Object value) {
        values.add(value);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof GenericArrayData2 that)) {
            return false;
        }
        if (values.size() != that.values.size()) {
            return false;
        }
        for (int i = 0; i < values.size(); i++) {
            if (!Objects.deepEquals(values.get(i), that.values.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = 1;
        for (Object value : values) {
            result = 31 * result + Objects.hashCode(value);
        }
        return result;
    }
}
