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

import java.util.*;

/** StreamOperation. */
public final class StreamOperation implements Operation {

  private final StreamType streamType;
  private final String name;
  private final String source;
  private final List<StreamOperatorOperation> streamOperatorOperations;
  private final Map<String, String> sourceProperties;
  private final Map<String, String> nameProperties;

  public StreamOperation(
      StreamType streamType,
      String name,
      String source,
      List<StreamOperatorOperation> streamOperatorOperations,
      Map<String, String> sourceProperties,
      Map<String, String> nameProperties) {
    this.streamType = streamType;
    this.name = name;
    this.source = source;
    this.streamOperatorOperations =
        streamOperatorOperations == null ? new ArrayList<>() : streamOperatorOperations;
    this.sourceProperties = sourceProperties == null ? new HashMap<>() : sourceProperties;
    this.nameProperties = nameProperties == null ? new HashMap<>() : nameProperties;
  }

  @Override
  public String asSummaryString() {
    final var args = new LinkedHashMap<String, Object>();
    args.put("name", name);
    args.put("source", source);
    return OperationUtils.formatWithChildren(
        streamType.name(), args, streamOperatorOperations, Operation::asSummaryString);
  }

  public StreamType streamType() {
    return streamType;
  }

  public String name() {
    return name;
  }

  public String source() {
    return source;
  }

  public List<StreamOperatorOperation> streamOperatorOperations() {
    return streamOperatorOperations;
  }

  public Map<String, String> sourceProperties() {
    return sourceProperties;
  }

  public Map<String, String> nameProperties() {
    return nameProperties;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    var that = (StreamOperation) obj;
    return Objects.equals(this.streamType, that.streamType)
        && Objects.equals(this.name, that.name)
        && Objects.equals(this.source, that.source)
        && Objects.equals(this.streamOperatorOperations, that.streamOperatorOperations)
        && Objects.equals(this.sourceProperties, that.sourceProperties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(streamType, name, source, streamOperatorOperations, sourceProperties);
  }

  @Override
  public String toString() {
    return "StreamOperation["
        + "streamType="
        + streamType
        + ", "
        + "name="
        + name
        + ", "
        + "source="
        + source
        + ", "
        + "streamOperatorOperations="
        + streamOperatorOperations
        + ", "
        + "sourceProperties="
        + sourceProperties
        + ']';
  }
}
