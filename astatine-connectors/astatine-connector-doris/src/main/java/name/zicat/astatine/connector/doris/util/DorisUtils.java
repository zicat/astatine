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

package name.zicat.astatine.connector.doris.util;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.codec.binary.Base64;
import org.apache.flink.table.types.logical.*;
import org.apache.http.HttpHeaders;

import static name.zicat.astatine.connector.doris.table.DorisConfigOptions.AUTO_CREATE_TABLE_PROPERTIES;

/** DorisUtils. */
public class DorisUtils {

  public static final String HEAD_KEY_LABEL = "label";
  public static final String HEAD_KEY_GROUP_COMMIT = "group_commit";

  private static final Pattern ENGINE_PATTERN =
      Pattern.compile("\\b(DUPLICATE|UNIQUE|AGGREGATE)\\s+KEY\\s*\\(([^)]+)\\)");

  public static List<String> getFieldsFromEngine(String engine) {
    Matcher matcher = ENGINE_PATTERN.matcher(engine);
    if (matcher.find()) {
      String fields = matcher.group(2);
      return Arrays.stream(fields.split(",")).map(String::trim).toList();
    }
    return List.of();
  }

  public static String createFieldsDDLSql(
      List<RowType.RowField> fields, Map<String, String> autoCreateFieldsFunctions) {
    final var it = fields.iterator();
    final var sql = new StringBuilder();
    while (it.hasNext()) {
      final var rowField = it.next();
      final var function = autoCreateFieldsFunctions.get(rowField.getName());
      if (function == null) {
        sql.append("    `%s`    %s".formatted(rowField.getName(), columnDDL(rowField.getType())));
      } else {
        sql.append(
            "    `%s`    %s   %s"
                .formatted(rowField.getName(), columnDDL(rowField.getType()), function));
      }
      if (it.hasNext()) {
        sql.append(",");
        sql.append(System.lineSeparator());
      }
    }
    return sql.toString();
  }

  public static String createPropertiesDDLSql(Map<String, String> properties) {
    final var it = properties.entrySet().iterator();
    final var sql = new StringBuilder();
    while (it.hasNext()) {
      final var entry = it.next();
      sql.append(
          "    \"%s\" = \"%s\""
              .formatted(
                  entry.getKey().substring(AUTO_CREATE_TABLE_PROPERTIES.length()),
                  entry.getValue()));
      if (it.hasNext()) {
        sql.append(",");
        sql.append(System.lineSeparator());
      }
    }
    return sql.toString();
  }

  private static StringBuilder columnDDL(LogicalType rowType) {
    final var sql = new StringBuilder();
    final var rootType = rowType.getTypeRoot();
    if (rootType == LogicalTypeRoot.MAP) {
      // use string json to store map type
      sql.append(
          "Map<%s,%s>"
              .formatted(
                  columnDDL(((MapType) rowType).getKeyType()),
                  columnDDL(((MapType) rowType).getValueType())));
    } else if (rootType == LogicalTypeRoot.ARRAY) {
      sql.append("Array<");
      sql.append(columnDDL(rowType.getChildren().get(0)));
      sql.append(">");
    } else if (rootType == LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE
        || rootType == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
        || rootType == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      sql.append("DATETIME");
    } else if (rootType == LogicalTypeRoot.VARCHAR) {
      sql.append("VARCHAR(%d)".formatted(Math.min(255, ((VarCharType) rowType).getLength())));
    } else {
      sql.append(rowType.getTypeRoot().name().toUpperCase());
    }
    return sql;
  }

  /**
   * create stream load heads.
   *
   * @param csvColumnSeparator csvColumnSeparator
   * @param csvLineDelimiter csvLineDelimiter
   * @return Properties
   */
  public static Map<String, String> streamLoadHeads(
      String csvColumnSeparator,
      String csvLineDelimiter,
      String[] fieldNames,
      String username,
      String password,
      Map<String, String> headers) {
    final var columns =
        Arrays.stream(fieldNames)
            .map(item -> String.format("`%s`", item.trim().replace("`", "")))
            .collect(Collectors.joining(","));
    final var allHeaders = new HashMap<>(headers);
    allHeaders.put("columns", columns);
    allHeaders.put("format", "csv");
    allHeaders.put("column_separator", csvColumnSeparator);
    allHeaders.put("line_delimiter", csvLineDelimiter);
    allHeaders.put(HttpHeaders.EXPECT, "100-continue");
    allHeaders.put(HttpHeaders.AUTHORIZATION, basicAuthHeader(username, password));
    return allHeaders;
  }

  /**
   * set basic auth header.
   *
   * @param username username
   * @param password password
   * @return String
   */
  private static String basicAuthHeader(String username, String password) {
    final var tobeEncode = username + ":" + password;
    final var encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
    return "Basic " + new String(encoded);
  }
}
