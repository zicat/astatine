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

package name.zicat.astatine.streaming.sql.runtime.map;

import name.zicat.astatine.streaming.sql.parser.function.MapFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformContext;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import static org.apache.flink.table.types.utils.TypeInfoDataTypeConverter.toDataType;

/** Row2PojoMapFunctionFactory. */
public class Row2PojoMapFunctionFactory<T> implements MapFunctionFactory<Row, T> {

  public static final String IDENTITY = "row_2_pojo";

  public static final ConfigOption<String> OPTION_MAPPING_CLASS_NAME =
      ConfigOptions.key("mapping.class").stringType().defaultValue(null);

  public static final ConfigOption<String> OPTION_RETURN_CLASS_NAME =
      ConfigOptions.key("return.class").stringType().defaultValue(null);

  @SuppressWarnings("unchecked")
  @Override
  public DataStream<T> transform(TransformContext context, DataStream<Row> stream) {

    final var dataTypeFactory =
        context.streamTableEnvironmentImpl().getCatalogManager().getDataTypeFactory();
    final var rowType = (RowType) toDataType(dataTypeFactory, stream.getType()).getLogicalType();
    final var pojoClassName = context.get(OPTION_MAPPING_CLASS_NAME);
    final var returnClassName = context.get(OPTION_RETURN_CLASS_NAME);
    final Class<?> pojoClass;
    final Class<?> returnClass;
    try {
      pojoClass = Class.forName(pojoClassName);
      returnClass = returnClassName == null ? pojoClass : Class.forName(returnClassName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    final var result = stream.map(new Row2PojoMapFunction<T>(rowType, pojoClassName));
    return setReturn(
        (TypeInformation<T>) TypeInformation.of(returnClass),
        result.name(identity() + "_" + result.getId()));
  }

  @Override
  public String identity() {
    return IDENTITY;
  }

  @Override
  public MapFunction<Row, T> createMap(TransformContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypeInformation<T> returns() {
    throw new UnsupportedOperationException();
  }
}
