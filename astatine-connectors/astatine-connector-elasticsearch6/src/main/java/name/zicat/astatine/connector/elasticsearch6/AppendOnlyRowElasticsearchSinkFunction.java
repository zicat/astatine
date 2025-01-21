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
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.table.data.RowData;

import java.util.function.Function;

import javax.annotation.Nullable;

/** AppendOnlyRowElasticsearchSinkFunction. */
@SuppressWarnings("deprecation")
public class AppendOnlyRowElasticsearchSinkFunction extends DefaultRowElasticsearchSinkFunction {
  public AppendOnlyRowElasticsearchSinkFunction(
      IndexGenerator indexGenerator,
      IndexGenerator routingGenerator,
      @Nullable String docType,
      SerializationSchema<RowData> serializationSchema,
      XContentType contentType,
      Function<RowData, String> createKey) {
    super(indexGenerator, routingGenerator, docType, serializationSchema, contentType, createKey);
  }

  @Override
  public void processUpsert(RowData row, RequestIndexer indexer) {
    final var document = serializationSchema.serialize(row);
    final var key = createKey.apply(row);
    final var routing = routingGenerator.generate(row);
    final var indexRequest =
        REQUEST_FACTORY.createIndexRequest(
            indexGenerator.generate(row), docType, key, contentType, document, routing);
    indexer.add(indexRequest);
  }
}
