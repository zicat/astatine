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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.action.delete.DeleteRequest;
import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.action.index.IndexRequest;
import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.action.update.UpdateRequest;
import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.common.xcontent.XContentType;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.io.Serial;
import java.util.Objects;
import java.util.function.Function;

import javax.annotation.Nullable;

/** DefaultRowElasticsearchSinkFunction. */
@SuppressWarnings("deprecation")
public class DefaultRowElasticsearchSinkFunction implements RowElasticsearchSinkFunction {

  protected static final RequestFactory REQUEST_FACTORY = new Elasticsearch6RequestFactory();
  @Serial private static final long serialVersionUID = 1L;
  protected final IndexGenerator indexGenerator;
  protected final IndexGenerator routingGenerator;
  protected final String docType;
  protected final SerializationSchema<RowData> serializationSchema;
  protected final XContentType contentType;
  protected final Function<RowData, String> createKey;

  public DefaultRowElasticsearchSinkFunction(
      IndexGenerator indexGenerator,
      IndexGenerator routingGenerator,
      @Nullable String docType,
      SerializationSchema<RowData> serializationSchema,
      XContentType contentType,
      Function<RowData, String> createKey) {
    this.indexGenerator = Preconditions.checkNotNull(indexGenerator);
    this.routingGenerator = Preconditions.checkNotNull(routingGenerator);
    this.docType = docType;
    this.serializationSchema = Preconditions.checkNotNull(serializationSchema);
    this.contentType = Preconditions.checkNotNull(contentType);
    this.createKey = Preconditions.checkNotNull(createKey);
  }

  @Override
  public void open(RuntimeContext ctx) throws Exception {
    serializationSchema.open(RuntimeContextInitializationContextAdapters.serializationAdapter(ctx));
    indexGenerator.open();
  }

  @Override
  public void processUpsert(RowData row, RequestIndexer indexer) {
    final var document = serializationSchema.serialize(row);
    final var key = createKey.apply(row);
    final var routing = routingGenerator.generate(row);
    if (key != null) {
      final var updateRequest =
          REQUEST_FACTORY.createUpdateRequest(
              indexGenerator.generate(row), docType, key, contentType, document, routing);
      indexer.add(updateRequest);
    } else {
      final var indexRequest =
          REQUEST_FACTORY.createIndexRequest(
              indexGenerator.generate(row), docType, null, contentType, document, routing);
      indexer.add(indexRequest);
    }
  }

  @Override
  public void processDelete(RowData row, RequestIndexer indexer) {
    final var key = createKey.apply(row);
    final var routing = routingGenerator.generate(row);
    final var deleteRequest =
        REQUEST_FACTORY.createDeleteRequest(indexGenerator.generate(row), docType, key, routing);
    indexer.add(deleteRequest);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o != null && getClass() == o.getClass()) {
      DefaultRowElasticsearchSinkFunction that = (DefaultRowElasticsearchSinkFunction) o;
      return Objects.equals(indexGenerator, that.indexGenerator)
          && Objects.equals(docType, that.docType)
          && Objects.equals(serializationSchema, that.serializationSchema)
          && contentType == that.contentType
          && Objects.equals(createKey, that.createKey);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(indexGenerator, docType, serializationSchema, contentType, createKey);
  }

  private static class Elasticsearch6RequestFactory implements RequestFactory {
    private Elasticsearch6RequestFactory() {}

    @Override
    public UpdateRequest createUpdateRequest(
        String index,
        String docType,
        String key,
        XContentType contentType,
        byte[] document,
        String routing) {
      return (new UpdateRequest(index, docType, key))
          .doc(document, contentType)
          .upsert(document, contentType)
          .routing(routing);
    }

    @Override
    public IndexRequest createIndexRequest(
        String index,
        String docType,
        String key,
        XContentType contentType,
        byte[] document,
        String routing) {
      return (new IndexRequest(index, docType, key)).source(document, contentType).routing(routing);
    }

    @Override
    public DeleteRequest createDeleteRequest(
        String index, String docType, String key, String routing) {
      return new DeleteRequest(index, docType, key).routing(routing);
    }
  }
}
