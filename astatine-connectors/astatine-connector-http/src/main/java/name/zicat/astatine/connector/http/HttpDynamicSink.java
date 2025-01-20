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

package name.zicat.astatine.connector.http;

import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;
import static org.apache.flink.util.Preconditions.checkNotNull;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.*;
import java.util.stream.Stream;

import javax.annotation.Nullable;

/** HttpDynamicSink. */
public class HttpDynamicSink implements DynamicTableSink, SupportsWritingMetadata {

  /** Metadata that is appended at the end of a physical sink row. */
  protected List<String> metadataKeys;

  /** Data type of consumed data type. */
  protected DataType consumedDataType;

  /** Data type to configure the formats. */
  protected final DataType physicalDataType;

  /** Parallelism of the physical Kafka producer. * */
  protected final ReadableConfig tableOptions;

  public HttpDynamicSink(
      DataType consumedDataType, DataType physicalDataType, ReadableConfig tableOptions) {
    this.consumedDataType = checkNotNull(consumedDataType, "Consumed data type must not be null.");
    this.physicalDataType = checkNotNull(physicalDataType, "Physical data type must not be null.");
    this.metadataKeys = Collections.emptyList();
    this.tableOptions = tableOptions;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
    return requestedMode;
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    checkURLMetadataKeys();
    final List<LogicalType> physicalChildren = physicalDataType.getLogicalType().getChildren();
    final int[] metadataPositions = getMetadataPositions(physicalChildren);
    final Integer parallelism = tableOptions.getOptional(SINK_PARALLELISM).orElse(null);
    return SinkFunctionProvider.of(
        new HttpSinkFunction(metadataPositions, tableOptions), parallelism);
  }

  @Override
  public DynamicTableSink copy() {
    return new HttpDynamicSink(consumedDataType, physicalDataType, tableOptions);
  }

  @Override
  public String asSummaryString() {
    return "Http table sink";
  }

  @Override
  public Map<String, DataType> listWritableMetadata() {
    final Map<String, DataType> metadataMap = new LinkedHashMap<>();
    Stream.of(HttpWritableMetadata.values())
        .forEachOrdered(m -> metadataMap.put(m.key, m.dataType));
    return metadataMap;
  }

  @Override
  public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
    this.metadataKeys = metadataKeys;
    this.consumedDataType = consumedDataType;
  }

  private @Nullable SerializationSchema<RowData> createSerialization(
      Context context,
      @Nullable EncodingFormat<SerializationSchema<RowData>> format,
      int[] projection) {
    if (format == null) {
      return null;
    }
    DataType physicalFormatDataType = Projection.of(projection).project(this.physicalDataType);
    return format.createRuntimeEncoder(context, physicalFormatDataType);
  }

  private RowData.FieldGetter[] getFieldGetters(
      List<LogicalType> physicalChildren, int[] keyProjection) {
    return Arrays.stream(keyProjection)
        .mapToObj(
            targetField ->
                RowData.createFieldGetter(physicalChildren.get(targetField), targetField))
        .toArray(RowData.FieldGetter[]::new);
  }

  private int[] getMetadataPositions(List<LogicalType> physicalChildren) {
    return Stream.of(HttpWritableMetadata.values())
        .mapToInt(
            m -> {
              final int pos = metadataKeys.indexOf(m.key);
              if (pos < 0) {
                return -1;
              }
              return physicalChildren.size() + pos;
            })
        .toArray();
  }

  private void checkURLMetadataKeys() {
    if (!metadataKeys.contains(HttpWritableMetadata.URL.key)) {
      throw new IllegalArgumentException(
          "sink table must contains column defined '`url` STRING METADATA'");
    }
  }
}
