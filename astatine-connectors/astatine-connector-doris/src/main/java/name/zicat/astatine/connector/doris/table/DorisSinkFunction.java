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

package name.zicat.astatine.connector.doris.table;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import name.zicat.astatine.connector.doris.model.GroupCommitMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.TextNode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.IOUtils;

import static name.zicat.astatine.connector.doris.table.DorisConfigOptions.*;
import static name.zicat.astatine.connector.doris.util.DorisJDBCUtils.executeSql;
import static name.zicat.astatine.connector.doris.util.DorisUtils.*;
import static name.zicat.astatine.connector.doris.util.EscapingUtils.escaping;

/** DorisSinkFunction. */
public class DorisSinkFunction extends RichSinkFunction<RowData> implements CheckpointedFunction {

  private static final String NULL_VALUE = "\\N";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final String[] fieldNames;

  private final String columnSeparator;
  private final String lineDelimiter;
  private final long connectionTimeout;
  private final long socketTimeout;
  private final String fenodes;
  private final String username;
  private final String password;
  private final String db;
  private final String table;
  private final long flushInterval;
  private final int bufferFlushMaxBytes;
  private final int maxRetries;
  private final long retryInterval;
  private final GroupCommitMode groupCommitMode;
  private final int threads;
  private final DynamicTableSink.DataStructureConverter converter;
  private final DataType dataType;
  private final Map<String, String> autoCreateTableProperties;
  private final Map<String, String> autoCreateFieldsFunctions;
  private transient DorisHealthChecker dorisHealthChecker;
  private transient DorisStreamLoadContext dorisStreamLoadContext;
  private final Map<String, String> headers;

  public DorisSinkFunction(
      ReadableConfig config,
      String[] fieldNames,
      DynamicTableSink.DataStructureConverter converter,
      Map<String, String> autoCreateTableProperties,
      Map<String, String> autoCreateFieldsFunctions,
      DataType dataType,
      Map<String, String> headers) {
    this.db = db(config);
    this.table = table(config);
    this.converter = converter;
    this.fieldNames = fieldNames;
    this.columnSeparator = escaping(config.get(SINK_COLUMN_SEPARATOR));
    this.lineDelimiter = escaping(config.get(SINK_LINE_DELIMITER));
    this.connectionTimeout = config.get(CONNECTION_TIMEOUT).toMillis();
    this.socketTimeout = config.get(SOCKET_TIMEOUT).toMillis();
    this.fenodes = config.get(FENODES);
    this.username = config.get(USERNAME);
    this.password = config.get(PASSWORD);
    this.flushInterval = config.get(SINK_FLUSH_INTERVAL).toMillis();
    this.bufferFlushMaxBytes = config.get(SINK_BUFFER_FLUSH_MAX_BYTES);
    this.groupCommitMode = groupCommitMode(config);
    this.maxRetries = config.get(SINK_RETRY_TIMES);
    this.retryInterval = config.get(SINK_RETRY_INTERVAL).toMillis();
    this.threads = config.get(SINK_THREADS);
    this.autoCreateTableProperties = autoCreateTableProperties;
    this.autoCreateFieldsFunctions = autoCreateFieldsFunctions;
    this.dataType = dataType;
    this.headers = headers;
  }

  @Override
  public void open(Configuration parameters) throws SQLException {
    autoCreateTable();
    this.dorisHealthChecker = new DorisHealthChecker(fenodes, username, password, db, table);
    this.dorisStreamLoadContext =
        new DorisStreamLoadContext(
            dorisHealthChecker,
            bufferFlushMaxBytes,
            threads,
            groupCommitMode,
            streamLoadHeads(
                columnSeparator, lineDelimiter, fieldNames, username, password, headers),
            connectionTimeout,
            socketTimeout,
            maxRetries,
            retryInterval,
            flushInterval);
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) {
    dorisStreamLoadContext.flush();
  }

  @Override
  public void initializeState(FunctionInitializationContext context) {}

  @Override
  public void invoke(RowData value, Context context) throws Exception {
    dorisStreamLoadContext.checkState();
    final var row = (Row) converter.toExternal(value);
    if (row != null) {
      dorisStreamLoadContext.invoke(row2CSV(row));
    }
  }

  @Override
  public void close() {
    try {
      if (dorisStreamLoadContext != null) {
        dorisStreamLoadContext.flush();
      }
    } finally {
      IOUtils.closeQuietly(dorisHealthChecker);
      IOUtils.closeQuietly(dorisStreamLoadContext);
    }
  }

  /**
   * row to csv.
   *
   * @param rowData rowData
   * @return byte array
   */
  private byte[] row2CSV(Row rowData) throws Exception {
    final var stringJoiner = new StringJoiner(columnSeparator);
    for (var i = 0; i < rowData.getArity(); i++) {
      final var field = rowData.getField(i);
      if (field == null) {
        stringJoiner.add(NULL_VALUE);
      } else if (field.getClass().isArray()) {
        var data = Arrays.toString((Object[]) field);
        stringJoiner.add(replaceIllegalCharacters(data));
      } else if (field instanceof Map<?, ?> map) {
        final var node = OBJECT_MAPPER.createObjectNode();
        for (var entry : map.entrySet()) {
          if (entry.getValue() == null) {
            continue;
          }
          final var strValue = entry.getValue().toString();
          try {
            node.set(entry.getKey().toString(), OBJECT_MAPPER.readTree(strValue));
          } catch (Exception e) {
            node.set(entry.getKey().toString(), TextNode.valueOf(strValue));
          }
        }
        var data = OBJECT_MAPPER.writeValueAsString(node);
        stringJoiner.add(replaceIllegalCharacters(data));
      } else {
        var data = field.toString();
        stringJoiner.add(replaceIllegalCharacters(data));
      }
    }
    return (stringJoiner + lineDelimiter).getBytes(StandardCharsets.UTF_8);
  }

  private String replaceIllegalCharacters(String data) {
    if (data.contains(columnSeparator)) {
      data = data.replace(columnSeparator, "");
    }
    if (data.contains(lineDelimiter)) {
      data = data.replace(lineDelimiter, "");
    }
    return data;
  }

  private void autoCreateTable() throws SQLException {
    final var autoCreateTable = autoCreateTableProperties.get(AUTO_CREATE_TABLE.key());
    if (!Boolean.parseBoolean(autoCreateTable)) {
      return;
    }
    final var engine = autoCreateTableProperties.get(AUTO_CREATE_TABLE_ENGINE.key());
    if (engine == null || engine.isBlank()) {
      throw new RuntimeException("auto create table engine is null");
    }
    final var partition = autoCreateTableProperties.get(AUTO_CREATE_TABLE_PARTITION.key());
    final var bucket = autoCreateTableProperties.get(AUTO_CREATE_TABLE_BUCKET.key());
    if (bucket == null || bucket.isBlank()) {
      throw new RuntimeException("auto create table bucket is null");
    }
    final var properties =
        autoCreateTableProperties.entrySet().stream()
            .filter(v -> v.getKey().startsWith(AUTO_CREATE_TABLE_PROPERTIES))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    final var logicalType = (RowType) dataType.getLogicalType();
    final var keyFields = getFieldsFromEngine(engine);
    final var rowFields = new ArrayList<RowType.RowField>();
    for (var keyField : keyFields) {
      final var index = logicalType.getFieldIndex(keyField);
      if (index == -1) {
        throw new IllegalStateException("key field not found: " + keyField);
      }
      rowFields.add(logicalType.getFields().get(index));
    }
    for (var field : logicalType.getFields()) {
      if (!keyFields.contains(field.getName())) {
        rowFields.add(field);
      }
    }

    final var fieldsSql = createFieldsDDLSql(rowFields, autoCreateFieldsFunctions);

    final var sql =
        partition == null
            ? String.format(
                "CREATE TABLE IF NOT EXISTS `%s`.`%s` (%n"
                    + "%s  %n"
                    + ") %n"
                    + "%s %n"
                    + "%s %n"
                    + "properties(%n%s%n)",
                db, table, fieldsSql, engine, bucket, createPropertiesDDLSql(properties))
            : String.format(
                "CREATE TABLE IF NOT EXISTS `%s`.`%s` (%n"
                    + "%s  %n"
                    + ") %n"
                    + "%s %n"
                    + "%s %n"
                    + "%s %n"
                    + "properties(%n%s%n)",
                db,
                table,
                fieldsSql,
                engine,
                partition,
                bucket,
                createPropertiesDDLSql(properties));
    executeSql(fenodes, db, username, password, sql);
  }
}
