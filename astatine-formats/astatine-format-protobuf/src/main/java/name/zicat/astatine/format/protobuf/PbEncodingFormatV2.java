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

package name.zicat.astatine.format.protobuf;

import name.zicat.astatine.format.protobuf.serialize.PbRowDataSerializationSchemaV2;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

/** PbEncodingFormatV2. */
public class PbEncodingFormatV2 implements EncodingFormat<SerializationSchema<RowData>> {
  private final PbFormatConfig pbFormatConfig;

  public PbEncodingFormatV2(PbFormatConfig pbFormatConfig) {
    this.pbFormatConfig = pbFormatConfig;
  }

  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  public SerializationSchema<RowData> createRuntimeEncoder(
      DynamicTableSink.Context context, DataType consumedDataType) {
    RowType rowType = (RowType) consumedDataType.getLogicalType();
    return new PbRowDataSerializationSchemaV2(rowType, this.pbFormatConfig);
  }
}
