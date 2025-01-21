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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.elasticsearch6.shaded.org.apache.http.HttpHost;
import org.apache.flink.elasticsearch6.shaded.org.apache.http.auth.AuthScope;
import org.apache.flink.elasticsearch6.shaded.org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.flink.elasticsearch6.shaded.org.apache.http.client.CredentialsProvider;
import org.apache.flink.elasticsearch6.shaded.org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.client.RestClientBuilder;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch6.RestClientFactory;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.StringUtils;

import java.time.ZoneId;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

/** Elasticsearch6DynamicSink. */
@SuppressWarnings("deprecation")
public class Elasticsearch6DynamicSink implements DynamicTableSink {

  private final EncodingFormat<SerializationSchema<RowData>> format;
  private final TableSchema schema;
  private final Elasticsearch6Configuration config;
  private final ZoneId localTimeZoneId;
  private final boolean isDynamicIndexWithSystemTime;
  private final ElasticSearchBuilderProvider builderProvider;

  public Elasticsearch6DynamicSink(
      EncodingFormat<SerializationSchema<RowData>> format,
      Elasticsearch6Configuration config,
      TableSchema schema,
      ZoneId localTimeZoneId) {
    this(format, config, schema, localTimeZoneId, ElasticsearchSink.Builder::new);
  }

  Elasticsearch6DynamicSink(
      EncodingFormat<SerializationSchema<RowData>> format,
      Elasticsearch6Configuration config,
      TableSchema schema,
      ZoneId localTimeZoneId,
      ElasticSearchBuilderProvider builderProvider) {
    this.format = format;
    this.schema = schema;
    this.config = config;
    this.localTimeZoneId = localTimeZoneId;
    this.isDynamicIndexWithSystemTime = this.isDynamicIndexWithSystemTime();
    this.builderProvider = builderProvider;
  }

  public boolean isDynamicIndexWithSystemTime() {
    final var indexHelper = new IndexGeneratorFactory.IndexHelper();
    return indexHelper.checkIsDynamicIndexWithSystemTimeFormat(config.getIndex());
  }

  public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
    final var builder = ChangelogMode.newBuilder();
    for (RowKind kind : requestedMode.getContainedKinds()) {
      if (kind != RowKind.UPDATE_BEFORE) {
        builder.addContainedKind(kind);
      }
    }

    if (isDynamicIndexWithSystemTime && !requestedMode.containsOnly(RowKind.INSERT)) {
      throw new ValidationException(
          "Dynamic indexing based on system time only works on append only stream.");
    } else {
      return builder.build();
    }
  }

  public SinkFunctionProvider getSinkRuntimeProvider(Context context) {
    return () -> {
      final var upsertFunction =
          RowElasticsearchSinkFunctionFactory.findFactory(config.getSinkFunctionId())
              .create(format, config, schema, localTimeZoneId, builderProvider, context);
      final var builder = builderProvider.createBuilder(config.getHosts(), upsertFunction);
      builder.setFailureHandler(config.getFailureHandler());
      builder.setBulkFlushMaxActions(config.getBulkFlushMaxActions());
      builder.setBulkFlushMaxSizeMb((int) (config.getBulkFlushMaxByteSize() >> 20));
      builder.setBulkFlushInterval(config.getBulkFlushInterval());
      builder.setBulkFlushBackoff(config.isBulkFlushBackoffEnabled());
      config.getBulkFlushBackoffType().ifPresent(builder::setBulkFlushBackoffType);
      config.getBulkFlushBackoffRetries().ifPresent(builder::setBulkFlushBackoffRetries);
      config.getBulkFlushBackoffDelay().ifPresent(builder::setBulkFlushBackoffDelay);
      if (config.getUsername().isPresent()
          && config.getPassword().isPresent()
          && !StringUtils.isNullOrWhitespaceOnly(config.getUsername().get())
          && !StringUtils.isNullOrWhitespaceOnly(config.getPassword().get())) {
        builder.setRestClientFactory(
            new AuthRestClientFactory(
                config.getPathPrefix().orElse(null),
                config.getUsername().get(),
                config.getPassword().get()));
      } else {
        builder.setRestClientFactory(
            new DefaultRestClientFactory(config.getPathPrefix().orElse(null)));
      }

      final var sink = builder.build();
      if (config.isDisableFlushOnCheckpoint()) {
        sink.disableFlushOnCheckpoint();
      }
      return sink;
    };
  }

  public DynamicTableSink copy() {
    return this;
  }

  public String asSummaryString() {
    return "Elasticsearch6";
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o != null && getClass() == o.getClass()) {
      Elasticsearch6DynamicSink that = (Elasticsearch6DynamicSink) o;
      return Objects.equals(format, that.format)
          && Objects.equals(schema, that.schema)
          && Objects.equals(config, that.config)
          && Objects.equals(builderProvider, that.builderProvider);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hash(format, schema, config, builderProvider);
  }

  @VisibleForTesting
  static class AuthRestClientFactory implements RestClientFactory {
    private final String pathPrefix;
    private final String username;
    private final String password;
    private transient CredentialsProvider credentialsProvider;

    public AuthRestClientFactory(@Nullable String pathPrefix, String username, String password) {
      this.pathPrefix = pathPrefix;
      this.password = password;
      this.username = username;
    }

    public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
      if (pathPrefix != null) {
        restClientBuilder.setPathPrefix(pathPrefix);
      }

      if (credentialsProvider == null) {
        credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
            AuthScope.ANY, new UsernamePasswordCredentials(username, password));
      }

      restClientBuilder.setHttpClientConfigCallback(
          (httpAsyncClientBuilder) ->
              httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
    }

    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (o != null && getClass() == o.getClass()) {
        AuthRestClientFactory that = (AuthRestClientFactory) o;
        return Objects.equals(pathPrefix, that.pathPrefix)
            && Objects.equals(username, that.username)
            && Objects.equals(password, that.password);
      } else {
        return false;
      }
    }

    public int hashCode() {
      return Objects.hash(pathPrefix, username, password);
    }
  }

  @VisibleForTesting
  static class DefaultRestClientFactory implements RestClientFactory {
    private final String pathPrefix;

    public DefaultRestClientFactory(@Nullable String pathPrefix) {
      this.pathPrefix = pathPrefix;
    }

    public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
      if (pathPrefix != null) {
        restClientBuilder.setPathPrefix(pathPrefix);
      }
    }

    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (o != null && getClass() == o.getClass()) {
        DefaultRestClientFactory that = (DefaultRestClientFactory) o;
        return Objects.equals(pathPrefix, that.pathPrefix);
      } else {
        return false;
      }
    }

    public int hashCode() {
      return Objects.hash(pathPrefix);
    }
  }

  @FunctionalInterface
  public interface ElasticSearchBuilderProvider {
    ElasticsearchSink.Builder<RowData> createBuilder(
        List<HttpHost> hosts, RowElasticsearchSinkFunction function);
  }
}
