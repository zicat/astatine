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
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;

/** RowElasticsearchSinkFunction. */
@SuppressWarnings("deprecation")
public interface RowElasticsearchSinkFunction extends ElasticsearchSinkFunction<RowData> {

  @Override
  default void process(RowData element, RuntimeContext ctx, RequestIndexer indexer) {
    switch (element.getRowKind()) {
      case INSERT, UPDATE_AFTER -> processUpsert(element, indexer);
      case UPDATE_BEFORE, DELETE -> processDelete(element, indexer);
      default -> throw new TableException("Unsupported message kind: " + element.getRowKind());
    }
  }

  /**
   * process upsert.
   *
   * @param row row
   * @param indexer indexer
   */
  void processUpsert(RowData row, RequestIndexer indexer);

  /**
   * process delete.
   *
   * @param row row
   * @param indexer indexer
   */
  void processDelete(RowData row, RequestIndexer indexer);
}
