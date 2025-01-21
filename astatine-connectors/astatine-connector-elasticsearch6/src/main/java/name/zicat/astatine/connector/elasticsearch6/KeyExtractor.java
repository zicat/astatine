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

package name.zicat.astatine.connector.elasticsearch6;

import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Period;
import java.util.HashMap;
import java.util.Objects;
import java.util.function.Function;

/** KeyExtractor. */
@SuppressWarnings("deprecation")
public class KeyExtractor implements Function<RowData, String>, Serializable {
  private final FieldFormatter[] fieldFormatters;
  private final String keyDelimiter;

  private KeyExtractor(FieldFormatter[] fieldFormatters, String keyDelimiter) {
    this.fieldFormatters = fieldFormatters;
    this.keyDelimiter = keyDelimiter;
  }

  public String apply(RowData rowData) {
    StringBuilder builder = new StringBuilder();

    for (int i = 0; i < this.fieldFormatters.length; ++i) {
      if (i > 0) {
        builder.append(this.keyDelimiter);
      }

      String value = this.fieldFormatters[i].format(rowData);
      builder.append(value);
    }

    return builder.toString();
  }

  public static Function<RowData, String> createKeyExtractor(
      TableSchema schema, String keyDelimiter) {
    return schema
        .getPrimaryKey()
        .map(
            (key) -> {
              final var namesToColumns = new HashMap<String, ColumnWithIndex>();
              final var tableColumns = schema.getTableColumns();
              for (int i = 0; i < schema.getFieldCount(); ++i) {
                TableColumn column = tableColumns.get(i);
                namesToColumns.put(column.getName(), new ColumnWithIndex(column, i));
              }

              Objects.requireNonNull(namesToColumns);
              final var fieldFormatters =
                  key.getColumns().stream()
                      .map(namesToColumns::get)
                      .map((columnx) -> toFormatter(columnx.index, columnx.getType()))
                      .toArray(FieldFormatter[]::new);
              return (Function<RowData, String>) new KeyExtractor(fieldFormatters, keyDelimiter);
            })
        .orElseGet(NullKeyExtractor::new);
  }

  private static FieldFormatter toFormatter(int index, LogicalType type) {
    switch (type.getTypeRoot()) {
      case DATE -> {
        return (row) -> LocalDate.ofEpochDay(row.getInt(index)).toString();
      }
      case TIME_WITHOUT_TIME_ZONE -> {
        return (row) -> LocalTime.ofNanoOfDay((long) row.getInt(index) * 1000000L).toString();
      }
      case INTERVAL_YEAR_MONTH -> {
        return (row) -> Period.ofDays(row.getInt(index)).toString();
      }
      case INTERVAL_DAY_TIME -> {
        return (row) -> Duration.ofMillis(row.getLong(index)).toString();
      }
      case DISTINCT_TYPE -> {
        return toFormatter(index, ((DistinctType) type).getSourceType());
      }
      default -> {
        RowData.FieldGetter fieldGetter = RowData.createFieldGetter(type, index);
        return (row) -> Objects.requireNonNull(fieldGetter.getFieldOrNull(row)).toString();
      }
    }
  }

  @SuppressWarnings("deprecation")
  private static class ColumnWithIndex {
    public TableColumn column;
    public int index;

    public ColumnWithIndex(TableColumn column, int index) {
      this.column = column;
      this.index = index;
    }

    public LogicalType getType() {
      return this.column.getType().getLogicalType();
    }
  }

  private interface FieldFormatter extends Serializable {
    String format(RowData var1);
  }
}
