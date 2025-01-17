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

package name.zicat.astatine.streaming.sql.parser.transform;

import static name.zicat.astatine.streaming.sql.parser.function.FunctionFactory.OPTION_FUNCTION_IDENTITY;

import name.zicat.astatine.streaming.sql.parser.function.KeyByFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.utils.Types;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataTypeQueryable;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serial;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** KeyByTransformFactory. */
@SuppressWarnings({"unchecked"})
public class KeyByTransformFactory extends OneTransformFactory {

  private static final ConfigOption<String> OPTION_FIELD =
      ConfigOptions.key("fields").stringType().noDefaultValue();

  public static final String IDENTITY = "KEY BY";

  @Override
  public String identity() {
    return IDENTITY;
  }

  @SuppressWarnings("rawtypes")
  @Override
  protected Class<KeyByFunctionFactory> functionFactoryClass() {
    return KeyByFunctionFactory.class;
  }

  @Override
  public DataStream<?> transform(TransformContext context, DataStream<?> stream) {
    final var idOption = context.getOptional(OPTION_FUNCTION_IDENTITY);
    if (idOption.isPresent()) {
      return super.transform(context, stream);
    }
    final var type = stream.getType();
    final var fields =
        Arrays.stream(context.get(OPTION_FIELD).split(",")).map(String::trim).distinct().toList();
    if (fields.isEmpty()) {
      throw new IllegalStateException("fields not found");
    }
    if (type instanceof DataTypeQueryable dataTypeQueryable) {
      final var rowType = (RowType) dataTypeQueryable.getDataType().getLogicalType();
      final var fieldsIndex = Types.fieldsIndex(rowType, fields);
      final var fieldGetters = Types.fieldGetters(rowType, fieldsIndex);
      final var rowFields = Types.rowFields(rowType, fieldsIndex);
      final var returnType = new RowType(rowFields);
      final var rowStream = (DataStream<RowData>) stream;
      return rowStream.keyBy(
          new RowDataKeySelector(fieldGetters, InternalSerializers.create(returnType)),
          InternalTypeInfo.of(returnType));
    } else {
      throw new RuntimeException("KeyBy Fields only support RowData type, real : " + type);
    }
  }

  /** RowDataKeySelector. */
  public static final class RowDataKeySelector implements KeySelector<RowData, RowData> {
    @Serial private static final long serialVersionUID = 0L;
    private final List<RowData.FieldGetter> fieldGetters;
    private final RowDataSerializer keySerializer;

    /**
     * @param fieldGetters fieldGetters
     * @param keySerializer keySerializer
     */
    public RowDataKeySelector(
        List<RowData.FieldGetter> fieldGetters, RowDataSerializer keySerializer) {
      this.fieldGetters = fieldGetters;
      this.keySerializer = keySerializer;
    }

    @Override
    public RowData getKey(RowData row) {
      final var key = new GenericRowData(fieldGetters.size());
      for (int i = 0; i < fieldGetters.size(); i++) {
        key.setField(i, fieldGetters.get(i).getFieldOrNull(row));
      }
      return keySerializer.toBinaryRow(key).copy();
    }

    public List<RowData.FieldGetter> fieldGetters() {
      return fieldGetters;
    }

    public RowDataSerializer keySerializer() {
      return keySerializer;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj == null || obj.getClass() != this.getClass()) {
        return false;
      }
      var that = (RowDataKeySelector) obj;
      return Objects.equals(this.fieldGetters, that.fieldGetters)
          && Objects.equals(this.keySerializer, that.keySerializer);
    }

    @Override
    public int hashCode() {
      return Objects.hash(fieldGetters, keySerializer);
    }

    @Override
    public String toString() {
      return "RowDataKeySelector["
          + "fieldGetters="
          + fieldGetters
          + ", "
          + "keySerializer="
          + keySerializer
          + ']';
    }
  }
}
