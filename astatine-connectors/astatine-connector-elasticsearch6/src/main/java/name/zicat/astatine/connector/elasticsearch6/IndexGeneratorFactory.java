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

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

/** IndexGeneratorFactory. */
@SuppressWarnings("deprecation")
public class IndexGeneratorFactory {
  private IndexGeneratorFactory() {}

  public static IndexGenerator createIndexGenerator(
      String index, TableSchema schema, ZoneId localTimeZoneId) {
    final var indexHelper = new IndexHelper();
    return indexHelper.checkIsDynamicIndex(index)
        ? createRuntimeIndexGenerator(
            index, schema.getFieldNames(), schema.getFieldDataTypes(), indexHelper, localTimeZoneId)
        : new StaticIndexGenerator(index);
  }

  private static IndexGenerator createRuntimeIndexGenerator(
      String index,
      String[] fieldNames,
      DataType[] fieldTypes,
      IndexHelper indexHelper,
      final ZoneId localTimeZoneId) {
    final var dynamicIndexPatternStr = indexHelper.extractDynamicIndexPatternStr(index);
    final var indexPrefix = index.substring(0, index.indexOf(dynamicIndexPatternStr));
    final var indexSuffix = index.substring(indexPrefix.length() + dynamicIndexPatternStr.length());
    if (indexHelper.checkIsDynamicIndexWithSystemTimeFormat(index)) {
      final var dateTimeFormat =
          indexHelper.extractDateFormat(index, LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
      return new AbstractTimeIndexGenerator(index, dateTimeFormat) {
        public String generate(RowData row) {
          return indexPrefix
              .concat(LocalDateTime.now(localTimeZoneId).format(this.dateTimeFormatter))
              .concat(indexSuffix);
        }
      };
    } else {
      final var isDynamicIndexWithFormat = indexHelper.checkIsDynamicIndexWithFormat(index);
      final var indexFieldPos =
          indexHelper.extractIndexFieldPos(index, fieldNames, isDynamicIndexWithFormat);
      final var indexFieldType = fieldTypes[indexFieldPos].getLogicalType();
      final var indexFieldLogicalTypeRoot = indexFieldType.getTypeRoot();
      indexHelper.validateIndexFieldType(indexFieldLogicalTypeRoot);
      final RowData.FieldGetter fieldGetter =
          RowData.createFieldGetter(indexFieldType, indexFieldPos);
      if (isDynamicIndexWithFormat) {
        final var dateTimeFormat = indexHelper.extractDateFormat(index, indexFieldLogicalTypeRoot);
        final DynamicFormatter formatFunction =
            createFormatFunction(indexFieldType, indexFieldLogicalTypeRoot, localTimeZoneId);
        return new AbstractTimeIndexGenerator(index, dateTimeFormat) {
          public String generate(RowData row) {
            final var fieldOrNull = fieldGetter.getFieldOrNull(row);
            final var formattedField =
                fieldOrNull != null
                    ? formatFunction.format(fieldOrNull, dateTimeFormatter)
                    : "null";
            return indexPrefix.concat(formattedField).concat(indexSuffix);
          }
        };
      } else {
        return new IndexGeneratorBase(index) {
          public String generate(RowData row) {
            final var indexField = fieldGetter.getFieldOrNull(row);
            return indexPrefix
                .concat(indexField == null ? "null" : indexField.toString())
                .concat(indexSuffix);
          }
        };
      }
    }
  }

  private static DynamicFormatter createFormatFunction(
      LogicalType indexFieldType,
      LogicalTypeRoot indexFieldLogicalTypeRoot,
      ZoneId localTimeZoneId) {
    return switch (indexFieldLogicalTypeRoot) {
      case DATE ->
          (value, dateTimeFormatter) -> {
            Integer indexField = (Integer) value;
            return LocalDate.ofEpochDay((long) indexField).format(dateTimeFormatter);
          };
      case TIME_WITHOUT_TIME_ZONE ->
          (value, dateTimeFormatter) -> {
            Integer indexField = (Integer) value;
            return LocalTime.ofNanoOfDay((long) indexField * 1000000L).format(dateTimeFormatter);
          };
      case TIMESTAMP_WITHOUT_TIME_ZONE ->
          (value, dateTimeFormatter) -> {
            TimestampData indexField = (TimestampData) value;
            return indexField.toLocalDateTime().format(dateTimeFormatter);
          };
      case TIMESTAMP_WITH_TIME_ZONE ->
          throw new UnsupportedOperationException("TIMESTAMP_WITH_TIME_ZONE is not supported yet");
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE ->
          (value, dateTimeFormatter) -> {
            TimestampData indexField = (TimestampData) value;
            return indexField.toInstant().atZone(localTimeZoneId).format(dateTimeFormatter);
          };
      default ->
          throw new TableException(
              String.format(
                  "Unsupported type '%s' found in Elasticsearch dynamic index field, time-related pattern only support types are: DATE,TIME,TIMESTAMP.",
                  indexFieldType));
    };
  }

  public static class IndexHelper {
    private static final Pattern dynamicIndexPattern = Pattern.compile("\\{[^{}]+}?");
    private static final Pattern dynamicIndexTimeExtractPattern =
        Pattern.compile(".*\\{.+\\|.*}.*");
    private static final Pattern dynamicIndexSystemTimeExtractPattern =
        Pattern.compile(
            ".*\\{\\s*(now\\(\\s*\\)|NOW\\(\\s*\\)|current_timestamp|CURRENT_TIMESTAMP)\\s*\\|.*}.*");
    private static final List<LogicalTypeRoot> supportedTypes = new ArrayList<>();
    private static final Map<LogicalTypeRoot, String> defaultFormats = new HashMap<>();

    public IndexHelper() {}

    public void validateIndexFieldType(LogicalTypeRoot logicalType) {
      if (!supportedTypes.contains(logicalType)) {
        throw new IllegalArgumentException(
            String.format(
                "Unsupported type %s of index field, Supported types are: %s",
                logicalType, supportedTypes));
      }
    }

    public String getDefaultFormat(LogicalTypeRoot logicalType) {
      return (String) defaultFormats.get(logicalType);
    }

    @SuppressWarnings("StatementWithEmptyBody")
    public boolean checkIsDynamicIndex(String index) {
      Matcher matcher = dynamicIndexPattern.matcher(index);

      int count;
      for (count = 0; matcher.find(); ++count) {}

      if (count > 1) {
        throw new TableException(
            String.format(
                "Chaining dynamic index pattern %s is not supported, only support single dynamic index pattern.",
                index));
      } else {
        return count == 1;
      }
    }

    public boolean checkIsDynamicIndexWithFormat(String index) {
      return dynamicIndexTimeExtractPattern.matcher(index).matches();
    }

    public boolean checkIsDynamicIndexWithSystemTimeFormat(String index) {
      return dynamicIndexSystemTimeExtractPattern.matcher(index).matches();
    }

    public String extractDynamicIndexPatternStr(String index) {
      int start = index.indexOf("{");
      int end = index.lastIndexOf("}");
      return index.substring(start, end + 1);
    }

    public int extractIndexFieldPos(
        String index, String[] fieldNames, boolean isDynamicIndexWithFormat) {
      List<String> fieldList = Arrays.asList(fieldNames);
      String indexFieldName;
      if (isDynamicIndexWithFormat) {
        indexFieldName = index.substring(index.indexOf("{") + 1, index.indexOf("|"));
      } else {
        indexFieldName = index.substring(index.indexOf("{") + 1, index.indexOf("}"));
      }

      if (!fieldList.contains(indexFieldName)) {
        throw new TableException(
            String.format(
                "Unknown field '%s' in index pattern '%s', please check the field name.",
                indexFieldName, index));
      } else {
        return fieldList.indexOf(indexFieldName);
      }
    }

    private String extractDateFormat(String index, LogicalTypeRoot logicalType) {
      String format = index.substring(index.indexOf("|") + 1, index.indexOf("}"));
      if (format.isEmpty()) {
        format = this.getDefaultFormat(logicalType);
      }

      return format;
    }

    static {
      supportedTypes.add(LogicalTypeRoot.DATE);
      supportedTypes.add(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE);
      supportedTypes.add(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
      supportedTypes.add(LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE);
      supportedTypes.add(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
      supportedTypes.add(LogicalTypeRoot.VARCHAR);
      supportedTypes.add(LogicalTypeRoot.CHAR);
      supportedTypes.add(LogicalTypeRoot.TINYINT);
      supportedTypes.add(LogicalTypeRoot.INTEGER);
      supportedTypes.add(LogicalTypeRoot.BIGINT);
      defaultFormats.put(LogicalTypeRoot.DATE, "yyyy_MM_dd");
      defaultFormats.put(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE, "HH_mm_ss");
      defaultFormats.put(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, "yyyy_MM_dd_HH_mm_ss");
      defaultFormats.put(LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE, "yyyy_MM_dd_HH_mm_ss");
      defaultFormats.put(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE, "yyyy_MM_dd_HH_mm_ssX");
    }
  }

  interface DynamicFormatter extends Serializable {
    String format(@Nonnull Object var1, DateTimeFormatter var2);
  }
}
