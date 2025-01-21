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

import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.action.delete.DeleteRequest;
import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.action.index.IndexRequest;
import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.action.update.UpdateRequest;
import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.common.xcontent.XContentType;

import java.io.Serializable;

/** RequestFactory. */
public interface RequestFactory extends Serializable {

  UpdateRequest createUpdateRequest(
      String index,
      String type,
      String key,
      XContentType contentType,
      byte[] document,
      String routing);

  IndexRequest createIndexRequest(
      String index,
      String type,
      String key,
      XContentType contentType,
      byte[] document,
      String routing);

  DeleteRequest createDeleteRequest(String index, String type, String key, String routing);
}
