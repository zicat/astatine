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

import java.util.*;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import static name.zicat.astatine.connector.doris.table.DorisConfigOptions.*;

/**
 * The {@link DorisDynamicTableFactory} translates the catalog table to a table source.
 *
 * <p>Because the table source requires a decoding format, we are discovering the format using the
 * provided {@link FactoryUtil} for convenience.
 */
public final class DorisDynamicTableFactory implements DynamicTableSinkFactory {

  @Override
  public String factoryIdentifier() {
    return "doris";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    final var options = new HashSet<ConfigOption<?>>();
    options.add(FENODES);
    options.add(TABLE_IDENTIFIER);
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    final var options = new HashSet<ConfigOption<?>>();
    options.add(FENODES);
    options.add(TABLE_IDENTIFIER);
    options.add(USERNAME);
    options.add(PASSWORD);
    options.add(CONNECTION_TIMEOUT);
    options.add(SOCKET_TIMEOUT);
    options.add(SINK_RETRY_TIMES);
    options.add(SINK_GROUP_COMMIT);
    options.add(SINK_PARALLELISM);
    options.add(SINK_BUFFER_FLUSH_MAX_BYTES);
    options.add(SINK_COLUMN_SEPARATOR);
    options.add(SINK_LINE_DELIMITER);
    options.add(SINK_RETRY_INTERVAL);
    options.add(SINK_FLUSH_INTERVAL);
    options.add(SINK_THREADS);
    options.add(AUTO_CREATE_TABLE);
    options.add(AUTO_CREATE_TABLE_ENGINE);
    options.add(AUTO_CREATE_TABLE_PARTITION);
    options.add(AUTO_CREATE_TABLE_BUCKET);
    return options;
  }

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    final var helper = FactoryUtil.createTableFactoryHelper(this, context);
    helper.validateExcept(
        AUTO_CREATE_TABLE_PROPERTIES,
        HEADER_PROPERTIES,
        AUTO_CREATE_TABLE_ENGINE_AGGREGATE_FUNCTION,
        AUTO_CREATE_TABLE_FIELDS_TYPE);
    final var config = helper.getOptions();
    return new DorisDynamicTableSink(config, context.getCatalogTable());
  }
}
