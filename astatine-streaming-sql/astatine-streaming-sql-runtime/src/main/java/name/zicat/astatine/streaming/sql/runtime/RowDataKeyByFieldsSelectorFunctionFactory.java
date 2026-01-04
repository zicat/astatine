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

package name.zicat.astatine.streaming.sql.runtime;

import name.zicat.astatine.streaming.sql.parser.function.KeyByFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformContext;
import name.zicat.astatine.streaming.sql.parser.utils.Types;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serial;
import java.util.Arrays;

/** RowDataKeyByFieldsSelectorFunctionFactory. */
public class RowDataKeyByFieldsSelectorFunctionFactory
    implements KeyByFunctionFactory<RowData, RowData> {

  public static final String IDENTITY = "key_by_rowdata";

  public static final ConfigOption<String> OPTION_FIELD =
      ConfigOptions.key("fields").stringType().noDefaultValue();

  @Override
  public KeySelector<RowData, RowData> createKeySelector(TransformContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypeInformation<RowData> keyType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataStream<RowData> transform(TransformContext context, DataStream<RowData> stream) {
    final var fieldExpression = context.get(OPTION_FIELD);
    if (fieldExpression == null || fieldExpression.isBlank()) {
      throw new IllegalStateException("fields not found");
    }
    final var rowType = Types.toRowType(stream.getType());
    final var fieldNameTypes = Types.fieldsNameTypes(rowType, fieldExpression);
    final var rowFields =
        Arrays.stream(fieldNameTypes).map(Types.FieldNameType::targetRowField).toList();
    final var returnType = new RowType(rowFields);
    return stream.keyBy(
        new RowDataKeySelector(
            Arrays.stream(fieldNameTypes).mapToInt(Types.FieldNameType::getIndex).toArray(),
            InternalSerializers.create(returnType)),
        InternalTypeInfo.of(returnType));
  }

  @Override
  public String identity() {
    return IDENTITY;
  }

  /** RowDataKeySelector. */
  public static final class RowDataKeySelector implements KeySelector<RowData, RowData> {
    @Serial private static final long serialVersionUID = 0L;
    private final int[] projectMapping;
    private final RowDataSerializer keySerializer;
    private transient ProjectedRowData projectedRowData;

    /**
     * @param projectMapping projectMapping
     * @param keySerializer keySerializer
     */
    public RowDataKeySelector(int[] projectMapping, RowDataSerializer keySerializer) {
      this.projectMapping = projectMapping;
      this.keySerializer = keySerializer;
    }

    @Override
    public RowData getKey(RowData row) {
      if (projectedRowData == null) {
        projectedRowData = ProjectedRowData.from(projectMapping);
      }
      return keySerializer.toBinaryRow(projectedRowData.replaceRow(row)).copy();
    }
  }
}
