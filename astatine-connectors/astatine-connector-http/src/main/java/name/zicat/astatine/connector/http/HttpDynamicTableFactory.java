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

package name.zicat.astatine.connector.http;

import static name.zicat.astatine.connector.http.HttpTableOptions.*;

import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Preconditions;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

/** HttpDynamicTableFactory. */
public class HttpDynamicTableFactory implements DynamicTableSinkFactory {
  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    final DataType physicalDataType = context.getPhysicalRowDataType();
    final FactoryUtil.TableFactoryHelper helper =
        FactoryUtil.createTableFactoryHelper(this, autoCompleteSchemaRegistrySubject(context));
    final ReadableConfig tableOptions = helper.getOptions();
    FactoryUtil.validateFactoryOptions(this, tableOptions);
    return new HttpDynamicSink(physicalDataType, physicalDataType, tableOptions);
  }

  @Override
  public String factoryIdentifier() {
    return "http";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    options.add(REQUEST_TYPE);
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    options.add(FactoryUtil.FORMAT);
    options.add(SINK_PARALLELISM);
    options.add(PROXY);
    options.add(CONNECT_TIMEOUT);
    options.add(READ_TIMEOUT);
    options.add(RETRY_INTERVAL);
    options.add(RETRY_COUNT);
    options.add(ASYNC_QUEUE_SIZE);
    options.add(ASYNC_THREADS);
    return options;
  }

  /**
   * Creates an array of indices that determine which physical fields of the table schema to include
   * in the value format.
   */
  public static int[] createValueFormatProjection(DataType physicalDataType) {
    final LogicalType physicalType = physicalDataType.getLogicalType();
    Preconditions.checkArgument(physicalType.is(LogicalTypeRoot.ROW), "Row data type expected.");
    final int physicalFieldCount = LogicalTypeChecks.getFieldCount(physicalType);
    final IntStream physicalFields = IntStream.range(0, physicalFieldCount);
    return physicalFields.toArray();
  }

  public static Context autoCompleteSchemaRegistrySubject(Context context) {
    Map<String, String> tableOptions = context.getCatalogTable().getOptions();
    Map<String, String> newOptions = Configuration.fromMap(tableOptions).toMap();
    if (newOptions.size() > tableOptions.size()) {
      return new FactoryUtil.DefaultDynamicTableContext(
          context.getObjectIdentifier(),
          context.getCatalogTable().copy(newOptions),
          context.getEnrichmentOptions(),
          context.getConfiguration(),
          context.getClassLoader(),
          context.isTemporary());
    } else {
      return context;
    }
  }
}
