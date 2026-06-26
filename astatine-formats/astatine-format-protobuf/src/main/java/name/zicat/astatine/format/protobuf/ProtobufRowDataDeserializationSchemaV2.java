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

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;
import java.io.IOException;

import name.zicat.astatine.format.protobuf.parser.PBFactory;
import name.zicat.astatine.format.protobuf.parser.PBValueReader;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.*;

/** ProtobufRowDataDeserializationSchemaV2. */
public class ProtobufRowDataDeserializationSchemaV2 implements DeserializationSchema<RowData> {

  private final boolean ignoreParseErrors;
  private final String messageClassName;
  private final RowType rowType;
  private final TypeInformation<RowData> rowDataTypeInfo;
  private transient PBValueReader valueReader;

  public ProtobufRowDataDeserializationSchemaV2(
      boolean ignoreParseErrors,
      RowType rowType,
      TypeInformation<RowData> rowDataTypeInfo,
      String messageClassName) {
    this.ignoreParseErrors = ignoreParseErrors;
    this.rowType = rowType;
    this.rowDataTypeInfo = rowDataTypeInfo;
    this.messageClassName = messageClassName;
  }

  @Override
  public void open(InitializationContext context) throws Exception {
    final var method = Class.forName(messageClassName).getMethod("getDescriptor");
    method.setAccessible(true);
    final var descriptor = (Descriptors.Descriptor) method.invoke(null);
    final var factory = new PBFactory();
    valueReader = factory.createValueReader(descriptor, rowType);
  }

  @Override
  public RowData deserialize(byte[] message) throws IOException {
    try {
      final var stream = CodedInputStream.newInstance(message);
      return (RowData) valueReader.read(stream);
    } catch (Exception e) {
      if (ignoreParseErrors) {
        return null;
      }
      throw new IOException("Failed to deserialize counter message", e);
    }
  }

  @Override
  public boolean isEndOfStream(RowData nextElement) {
    return false;
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return rowDataTypeInfo;
  }
}
