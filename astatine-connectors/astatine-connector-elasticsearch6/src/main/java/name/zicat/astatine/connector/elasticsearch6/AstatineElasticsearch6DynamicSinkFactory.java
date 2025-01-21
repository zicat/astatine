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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.StringUtils;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static name.zicat.astatine.connector.elasticsearch6.ConfigurationUtils.getLocalTimeZoneId;

/** AstatineElasticsearch6DynamicSinkFactory. */
@SuppressWarnings("deprecation")
public class AstatineElasticsearch6DynamicSinkFactory implements DynamicTableSinkFactory {
  private static final Set<ConfigOption<?>> REQUIRED_OPTIONS;
  public static final Set<ConfigOption<?>> OPTIONAL_OPTIONS;

  public AstatineElasticsearch6DynamicSinkFactory() {}

  public DynamicTableSink createDynamicTableSink(Context context) {
    final var tableSchema = context.getCatalogTable().getSchema();
    final var helper = FactoryUtil.createTableFactoryHelper(this, context);
    final var format =
        helper.discoverEncodingFormat(
            SerializationFormatFactory.class, ElasticsearchConnectorOptions.FORMAT_OPTION);
    final var configuration = new Configuration();
    context.getCatalogTable().getOptions().forEach(configuration::setString);
    final var config = new Elasticsearch6Configuration(configuration, context.getClassLoader());
    validate(config, configuration);
    return new Elasticsearch6DynamicSink(
        format,
        config,
        TableSchemaUtils.getPhysicalSchema(tableSchema),
        getLocalTimeZoneId(context.getConfiguration()));
  }

  private void validate(Elasticsearch6Configuration config, Configuration originalConfiguration) {
    config.getFailureHandler();
    config.getHosts();
    validate(
        !config.getIndex().isEmpty(),
        () ->
            String.format(
                "'%s' must not be empty", ElasticsearchConnectorOptions.INDEX_OPTION.key()));
    final var maxActions = config.getBulkFlushMaxActions();
    validate(
        maxActions == -1 || maxActions >= 1,
        () ->
            String.format(
                "'%s' must be at least 1. Got: %s",
                ElasticsearchConnectorOptions.BULK_FLUSH_MAX_ACTIONS_OPTION.key(), maxActions));
    final var maxSize = config.getBulkFlushMaxByteSize();
    final var mb1 = 1048576L;
    validate(
        maxSize == -1L || maxSize >= mb1 && maxSize % mb1 == 0L,
        () ->
            String.format(
                "'%s' must be in MB granularity. Got: %s",
                ElasticsearchConnectorOptions.BULK_FLASH_MAX_SIZE_OPTION.key(),
                originalConfiguration
                    .get(ElasticsearchConnectorOptions.BULK_FLASH_MAX_SIZE_OPTION)
                    .toHumanReadableString()));
    validate(
        config
            .getBulkFlushBackoffRetries()
            .map(
                (retries) -> {
                  return retries >= 1;
                })
            .orElse(true),
        () ->
            String.format(
                "'%s' must be at least 1. Got: %s",
                ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION.key(),
                config.getBulkFlushBackoffRetries().get()));
    if (config.getUsername().isPresent()
        && !StringUtils.isNullOrWhitespaceOnly(config.getUsername().get())) {
      validate(
          config.getPassword().isPresent()
              && !StringUtils.isNullOrWhitespaceOnly(config.getPassword().get()),
          () ->
              String.format(
                  "'%s' and '%s' must be set at the same time. Got: username '%s' and password '%s'",
                  ElasticsearchConnectorOptions.USERNAME_OPTION.key(),
                  ElasticsearchConnectorOptions.PASSWORD_OPTION.key(),
                  config.getUsername().get(),
                  config.getPassword().orElse("")));
    }
  }

  private static void validate(boolean condition, Supplier<String> message) {
    if (!condition) {
      throw new ValidationException(message.get());
    }
  }

  public String factoryIdentifier() {
    return "astatine-elasticsearch-6";
  }

  public Set<ConfigOption<?>> requiredOptions() {
    return REQUIRED_OPTIONS;
  }

  public Set<ConfigOption<?>> optionalOptions() {
    return OPTIONAL_OPTIONS;
  }

  static {
    REQUIRED_OPTIONS =
        Stream.of(
                ElasticsearchConnectorOptions.HOSTS_OPTION,
                ElasticsearchConnectorOptions.INDEX_OPTION,
                ElasticsearchConnectorOptions.DOCUMENT_TYPE_OPTION)
            .collect(Collectors.toSet());
    OPTIONAL_OPTIONS = new HashSet<>();
    OPTIONAL_OPTIONS.addAll(
        Stream.of(
                ElasticsearchConnectorOptions.KEY_DELIMITER_OPTION,
                ElasticsearchConnectorOptions.FAILURE_HANDLER_OPTION,
                ElasticsearchConnectorOptions.FLUSH_ON_CHECKPOINT_OPTION,
                ElasticsearchConnectorOptions.BULK_FLASH_MAX_SIZE_OPTION,
                ElasticsearchConnectorOptions.BULK_FLUSH_MAX_ACTIONS_OPTION,
                ElasticsearchConnectorOptions.BULK_FLUSH_INTERVAL_OPTION,
                ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_TYPE_OPTION,
                ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION,
                ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_DELAY_OPTION,
                ElasticsearchConnectorOptions.CONNECTION_PATH_PREFIX,
                ElasticsearchConnectorOptions.FORMAT_OPTION,
                ElasticsearchConnectorOptions.PASSWORD_OPTION,
                ElasticsearchConnectorOptions.USERNAME_OPTION,
                ElasticsearchConfiguration.ROUTING_OPTION,
                ElasticsearchConfiguration.SINK_FUNCTION_ID_OPTION)
            .collect(Collectors.toSet()));
  }
}
