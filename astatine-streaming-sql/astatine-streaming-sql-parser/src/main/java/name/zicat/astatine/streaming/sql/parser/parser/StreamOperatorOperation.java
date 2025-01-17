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

package name.zicat.astatine.streaming.sql.parser.parser;

import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

/** StreamOperatorOperation. */
public final class StreamOperatorOperation implements Operation {

  private final String type;
  private final String source;
  private final Map<String, String> properties;

  public StreamOperatorOperation(
      String type, @Nullable String source, Map<String, String> properties) {
    this.type = type.toUpperCase();
    this.source = source;
    this.properties = properties;
  }

  @Override
  public String asSummaryString() {
    Map<String, Object> args = new LinkedHashMap<>();
    args.put("type", type);
    args.put("properties", properties);
    if (source != null) {
      args.put("source", source);
    }
    return OperationUtils.formatWithChildren(
        "OperationType", args, new ArrayList<>(), Operation::asSummaryString);
  }

  public String type() {
    return type;
  }

  @Nullable
  public String source() {
    return source;
  }

  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    var that = (StreamOperatorOperation) obj;
    return Objects.equals(this.type, that.type)
        && Objects.equals(this.source, that.source)
        && Objects.equals(this.properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, source, properties);
  }

  @Override
  public String toString() {
    return "StreamOperatorOperation["
        + "type="
        + type
        + ", "
        + "source="
        + source
        + ", "
        + "properties="
        + properties
        + ']';
  }
}
