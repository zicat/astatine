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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.common.xcontent.XContentType;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;

import java.time.ZoneId;

/** DefaultRowElasticsearchSinkFunctionFactory. */
@SuppressWarnings("deprecation")
public class DefaultRowElasticsearchSinkFunctionFactory
    implements RowElasticsearchSinkFunctionFactory {

  @Override
  public String identity() {
    return "default";
  }

  @SuppressWarnings("deprecation")
  @Override
  public RowElasticsearchSinkFunction create(
      EncodingFormat<SerializationSchema<RowData>> format,
      Elasticsearch6Configuration config,
      TableSchema schema,
      ZoneId localTimeZoneId,
      Elasticsearch6DynamicSink.ElasticSearchBuilderProvider builderProvider,
      DynamicTableSink.Context context) {
    final var encoder = format.createRuntimeEncoder(context, schema.toRowDataType());
    return new DefaultRowElasticsearchSinkFunction(
        createIndexGenerator(config, schema, localTimeZoneId),
        createRoutingGenerator(config, schema, localTimeZoneId),
        config.getDocumentType(),
        encoder,
        XContentType.JSON,
        KeyExtractor.createKeyExtractor(schema, config.getKeyDelimiter()));
  }
}
