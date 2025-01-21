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
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions;
import org.apache.flink.streaming.connectors.elasticsearch.util.IgnoringFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.util.NoOpFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.InstantiationUtil;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/** ElasticsearchConfiguration. */
@SuppressWarnings("deprecation")
public class ElasticsearchConfiguration {

  public static final ConfigOption<String> ROUTING_OPTION =
      ConfigOptions.key("routing")
          .stringType()
          .defaultValue(null)
          .withDescription("Elasticsearch routing for every record.");
  public static final ConfigOption<String> SINK_FUNCTION_ID_OPTION =
      ConfigOptions.key("function.id")
          .stringType()
          .defaultValue("default")
          .withDescription("Elasticsearch sink function");
  protected final ReadableConfig config;
  private final ClassLoader classLoader;

  ElasticsearchConfiguration(ReadableConfig config, ClassLoader classLoader) {
    this.config = config;
    this.classLoader = classLoader;
  }

  public ActionRequestFailureHandler getFailureHandler() {
    final var value = config.get(ElasticsearchConnectorOptions.FAILURE_HANDLER_OPTION);
    Object failureHandler;
    switch (value.toUpperCase()) {
      case "FAIL" -> failureHandler = new NoOpFailureHandler();
      case "IGNORE" -> failureHandler = new IgnoringFailureHandler();
      case "RETRY-REJECTED" -> failureHandler = new RetryRejectedExecutionFailureHandler();
      default -> {
        try {
          Class<?> failureHandlerClass = Class.forName(value, false, this.classLoader);
          failureHandler = InstantiationUtil.instantiate(failureHandlerClass);
        } catch (ClassNotFoundException var6) {
          throw new ValidationException(
              "Could not instantiate the failure handler class: " + value, var6);
        }
      }
    }
    return (ActionRequestFailureHandler) failureHandler;
  }

  public String getDocumentType() {
    return config.get(ElasticsearchConnectorOptions.DOCUMENT_TYPE_OPTION);
  }

  public int getBulkFlushMaxActions() {
    final var maxActions = config.get(ElasticsearchConnectorOptions.BULK_FLUSH_MAX_ACTIONS_OPTION);
    return maxActions == 0 ? -1 : maxActions;
  }

  public long getBulkFlushMaxByteSize() {
    final var maxSize =
        config.get(ElasticsearchConnectorOptions.BULK_FLASH_MAX_SIZE_OPTION).getBytes();
    return maxSize == 0L ? -1L : maxSize;
  }

  public long getBulkFlushInterval() {
    final var interval =
        config.get(ElasticsearchConnectorOptions.BULK_FLUSH_INTERVAL_OPTION).toMillis();
    return interval == 0L ? -1L : interval;
  }

  public Optional<String> getUsername() {
    return config.getOptional(ElasticsearchConnectorOptions.USERNAME_OPTION);
  }

  public Optional<String> getPassword() {
    return config.getOptional(ElasticsearchConnectorOptions.PASSWORD_OPTION);
  }

  public boolean isBulkFlushBackoffEnabled() {
    return config.get(ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_TYPE_OPTION)
        != ElasticsearchConnectorOptions.BackOffType.DISABLED;
  }

  public Optional<ElasticsearchSinkBase.FlushBackoffType> getBulkFlushBackoffType() {
    return switch (this.config.get(ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_TYPE_OPTION)) {
      case CONSTANT -> Optional.of(ElasticsearchSinkBase.FlushBackoffType.CONSTANT);
      case EXPONENTIAL -> Optional.of(ElasticsearchSinkBase.FlushBackoffType.EXPONENTIAL);
      default -> Optional.empty();
    };
  }

  public Optional<Integer> getBulkFlushBackoffRetries() {
    return config.getOptional(ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION);
  }

  public Optional<Long> getBulkFlushBackoffDelay() {
    return config
        .getOptional(ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_DELAY_OPTION)
        .map(Duration::toMillis);
  }

  public boolean isDisableFlushOnCheckpoint() {
    return !config.get(ElasticsearchConnectorOptions.FLUSH_ON_CHECKPOINT_OPTION);
  }

  public String getIndex() {
    return config.get(ElasticsearchConnectorOptions.INDEX_OPTION);
  }

  public String getRouting() {
    return config.get(ROUTING_OPTION);
  }

  public String getSinkFunctionId() {
    return config.get(SINK_FUNCTION_ID_OPTION);
  }

  public String getKeyDelimiter() {
    return config.get(ElasticsearchConnectorOptions.KEY_DELIMITER_OPTION);
  }

  public Optional<String> getPathPrefix() {
    return config.getOptional(ElasticsearchConnectorOptions.CONNECTION_PATH_PREFIX);
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o != null && getClass() == o.getClass()) {
      ElasticsearchConfiguration that = (ElasticsearchConfiguration) o;
      return Objects.equals(config, that.config) && Objects.equals(classLoader, that.classLoader);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hash(this.config, this.classLoader);
  }

  public ReadableConfig getConfig() {
    return config;
  }
}
