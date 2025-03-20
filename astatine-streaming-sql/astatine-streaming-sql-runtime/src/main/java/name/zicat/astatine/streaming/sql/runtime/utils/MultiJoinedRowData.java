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

import java.util.Arrays;
import java.util.Objects;
import org.apache.flink.table.data.*;
import org.apache.flink.types.RowKind;

/** MultiJoinedRowData. */
public class MultiJoinedRowData implements RowData {

  private RowKind rowKind = RowKind.INSERT;
  private RowData[] rows;

  public MultiJoinedRowData() {}

  public MultiJoinedRowData(RowData... rows) {
    this(RowKind.INSERT, rows);
  }

  public MultiJoinedRowData(RowKind rowKind, RowData... rows) {
    this.rowKind = rowKind;
    this.rows = rows;
  }

  public void replace(int index, RowData row) {
    rows[index] = row;
  }

  public MultiJoinedRowData replace(RowData... rows) {
    this.rows = rows;
    return this;
  }

  @Override
  public int getArity() {
    int arity = 0;
    for (RowData row : rows) {
      arity += row.getArity();
    }
    return arity;
  }

  @Override
  public RowKind getRowKind() {
    return rowKind;
  }

  @Override
  public void setRowKind(RowKind kind) {
    this.rowKind = kind;
  }

  @Override
  public boolean isNullAt(int pos) {
    for (RowData row : rows) {
      if (pos < row.getArity()) {
        return row.isNullAt(pos);
      } else {
        pos -= row.getArity();
      }
    }
    throw new RuntimeException("Invalid pos: " + pos);
  }

  @Override
  public boolean getBoolean(int pos) {
    for (RowData row : rows) {
      if (pos < row.getArity()) {
        return row.getBoolean(pos);
      } else {
        pos -= row.getArity();
      }
    }
    throw new RuntimeException("Invalid pos: " + pos);
  }

  @Override
  public byte getByte(int pos) {
    for (RowData row : rows) {
      if (pos < row.getArity()) {
        return row.getByte(pos);
      } else {
        pos -= row.getArity();
      }
    }
    throw new RuntimeException("Invalid pos: " + pos);
  }

  @Override
  public short getShort(int pos) {
    for (RowData row : rows) {
      if (pos < row.getArity()) {
        return row.getShort(pos);
      } else {
        pos -= row.getArity();
      }
    }
    throw new RuntimeException("Invalid pos: " + pos);
  }

  @Override
  public int getInt(int pos) {
    for (RowData row : rows) {
      if (pos < row.getArity()) {
        return row.getInt(pos);
      } else {
        pos -= row.getArity();
      }
    }
    throw new RuntimeException("Invalid pos: " + pos);
  }

  @Override
  public long getLong(int pos) {
    for (RowData row : rows) {
      if (pos < row.getArity()) {
        return row.getLong(pos);
      } else {
        pos -= row.getArity();
      }
    }
    throw new RuntimeException("Invalid pos: " + pos);
  }

  @Override
  public float getFloat(int pos) {
    for (RowData row : rows) {
      if (pos < row.getArity()) {
        return row.getFloat(pos);
      } else {
        pos -= row.getArity();
      }
    }
    throw new RuntimeException("Invalid pos: " + pos);
  }

  @Override
  public double getDouble(int pos) {
    for (RowData row : rows) {
      if (pos < row.getArity()) {
        return row.getDouble(pos);
      } else {
        pos -= row.getArity();
      }
    }
    throw new RuntimeException("Invalid pos: " + pos);
  }

  @Override
  public StringData getString(int pos) {
    for (RowData row : rows) {
      if (pos < row.getArity()) {
        return row.getString(pos);
      } else {
        pos -= row.getArity();
      }
    }
    throw new RuntimeException("Invalid pos: " + pos);
  }

  @Override
  public DecimalData getDecimal(int pos, int precision, int scale) {
    for (RowData row : rows) {
      if (pos < row.getArity()) {
        return row.getDecimal(pos, precision, scale);
      } else {
        pos -= row.getArity();
      }
    }
    throw new RuntimeException("Invalid pos: " + pos);
  }

  @Override
  public TimestampData getTimestamp(int pos, int precision) {
    for (RowData row : rows) {
      if (pos < row.getArity()) {
        return row.getTimestamp(pos, precision);
      } else {
        pos -= row.getArity();
      }
    }
    throw new RuntimeException("Invalid pos: " + pos);
  }

  @Override
  public <T> RawValueData<T> getRawValue(int pos) {
    for (RowData row : rows) {
      if (pos < row.getArity()) {
        return row.getRawValue(pos);
      } else {
        pos -= row.getArity();
      }
    }
    throw new RuntimeException("Invalid pos: " + pos);
  }

  @Override
  public byte[] getBinary(int pos) {
    for (RowData row : rows) {
      if (pos < row.getArity()) {
        return row.getBinary(pos);
      } else {
        pos -= row.getArity();
      }
    }
    throw new RuntimeException("Invalid pos: " + pos);
  }

  @Override
  public ArrayData getArray(int pos) {
    for (RowData row : rows) {
      if (pos < row.getArity()) {
        return row.getArray(pos);
      } else {
        pos -= row.getArity();
      }
    }
    throw new RuntimeException("Invalid pos: " + pos);
  }

  @Override
  public MapData getMap(int pos) {
    for (RowData row : rows) {
      if (pos < row.getArity()) {
        return row.getMap(pos);
      } else {
        pos -= row.getArity();
      }
    }
    throw new RuntimeException("Invalid pos: " + pos);
  }

  @Override
  public RowData getRow(int pos, int numFields) {
    for (RowData row : rows) {
      if (pos < row.getArity()) {
        return row.getRow(pos, numFields);
      } else {
        pos -= row.getArity();
      }
    }
    throw new RuntimeException("Invalid pos: " + pos);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MultiJoinedRowData that = (MultiJoinedRowData) o;
    var equals = Objects.equals(rowKind, that.rowKind) && rows.length == that.rows.length;
    if (!equals) {
      return false;
    }
    for (int i = 0; i < rows.length; i++) {
      if (!Objects.equals(rows[i], that.rows[i])) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(rowKind, Arrays.hashCode(rows));
  }
}
