// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package name.zicat.astatine.connector.doris.table;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.RowKind;

import static name.zicat.astatine.connector.doris.table.DorisConfigOptions.*;

/** DorisDynamicTableSink. */
@SuppressWarnings("deprecation")
public class DorisDynamicTableSink implements DynamicTableSink {

  private final ResolvedCatalogTable catalogTable;
  private final ReadableConfig config;

  public DorisDynamicTableSink(ReadableConfig config, ResolvedCatalogTable catalogTable) {
    this.config = config;
    this.catalogTable = catalogTable;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
    return ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT).build();
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    final var dataType = catalogTable.getResolvedSchema().toPhysicalRowDataType();
    final var function =
        new DorisSinkFunction(
            config,
            TableSchemaUtils.getPhysicalSchema(catalogTable.getSchema()).getFieldNames(),
            context.createDataStructureConverter(dataType),
            getDorisAutoCreateTableProperties(catalogTable.getOptions(), config),
            getDorisAutoCreateAggregationFieldFunctions(catalogTable.getOptions()),
            dataType,
            getHeaders(catalogTable.getOptions(), config));
    return SinkFunctionProvider.of(function, config.get(SINK_PARALLELISM));
  }

  public static Map<String, String> getDorisAutoCreateAggregationFieldFunctions(
      Map<String, String> tableOptions) {
    final var start = AUTO_CREATE_TABLE_ENGINE_AGGREGATE_FUNCTION.length();
    return tableOptions.entrySet().stream()
        .filter(e -> e.getKey().startsWith(AUTO_CREATE_TABLE_ENGINE_AGGREGATE_FUNCTION))
        .collect(
            HashMap::new,
            (m, e) -> m.put(e.getKey().substring(start), e.getValue()),
            HashMap::putAll);
  }

  public static Map<String, String> getDorisAutoCreateTableProperties(
      Map<String, String> tableOptions, ReadableConfig config) {
    final var properties =
        tableOptions.entrySet().stream()
            .filter(e -> e.getKey().startsWith(AUTO_CREATE_TABLE_PROPERTIES))
            .collect(
                () -> new HashMap<String, String>(),
                (m, e) -> m.put(e.getKey(), e.getValue()),
                HashMap::putAll);
    properties.put(AUTO_CREATE_TABLE.key(), String.valueOf(config.get(AUTO_CREATE_TABLE)));
    properties.put(AUTO_CREATE_TABLE_ENGINE.key(), config.get(AUTO_CREATE_TABLE_ENGINE));
    properties.put(AUTO_CREATE_TABLE_PARTITION.key(), config.get(AUTO_CREATE_TABLE_PARTITION));
    properties.put(AUTO_CREATE_TABLE_BUCKET.key(), config.get(AUTO_CREATE_TABLE_BUCKET));
    return properties;
  }

  public static Map<String, String> getHeaders(
      Map<String, String> tableOptions, ReadableConfig config) {
    return tableOptions.entrySet().stream()
        .filter(e -> e.getKey().startsWith(HEADER_PROPERTIES))
        .collect(
            HashMap::new,
            (m, e) -> m.put(e.getKey().substring(HEADER_PROPERTIES.length()), e.getValue()),
            HashMap::putAll);
  }

  @Override
  public DynamicTableSink copy() {
    return new DorisDynamicTableSink(config, catalogTable);
  }

  @Override
  public String asSummaryString() {
    return "Doris Table Sink";
  }
}
