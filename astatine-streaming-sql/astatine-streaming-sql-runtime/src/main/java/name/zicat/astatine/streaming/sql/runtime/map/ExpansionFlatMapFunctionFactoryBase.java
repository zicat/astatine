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

import java.util.List;
import name.zicat.astatine.streaming.sql.parser.function.FlatMapFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformContext;
import name.zicat.astatine.streaming.sql.parser.utils.Types;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/** ExpansionFlatMapFunctionFactoryBase. */
public abstract class ExpansionFlatMapFunctionFactoryBase
    implements FlatMapFunctionFactory<RowData, RowData> {

  public static final ConfigOption<String> OPTION_TIME_ZONE =
      ConfigOptions.key("timezone").stringType().defaultValue("GMT");

  public static final ConfigOption<String> OPTION_FIELD_START_TS =
      ConfigOptions.key("field.start-ts").stringType().defaultValue(null);
  public static final ConfigOption<String> OPTION_FIELD_END_TS =
      ConfigOptions.key("field.end-ts").stringType().defaultValue(null);
  public static final ConfigOption<String> OPTION_FIELD_CURRENT_TS =
      ConfigOptions.key("field.current-ts").stringType().noDefaultValue();
  public static final ConfigOption<Integer> OPTION_MAX_COUNT =
      ConfigOptions.key("max-count").intType().defaultValue(2);

  protected static final String DEFAULT_DATE_FIELD = "date";
  protected static final String DEFAULT_DATE_HOUR_FIELD = "hour";

  @Override
  public DataStream<RowData> transform(TransformContext context, DataStream<RowData> stream) {
    final var rowType = Types.toRowType(stream.getType());
    final var resultStream = stream.flatMap(create(context, rowType));
    return resultStream
        .returns(InternalTypeInfo.of(outputType(rowType)))
        .name(identity() + "_" + resultStream.getId());
  }

  protected abstract FlatMapFunction<RowData, RowData> create(
      TransformContext context, RowType inputType);

  protected abstract RowType outputType(RowType inputType);

  /**
   * check if the fields contains the specified field.
   *
   * @param fields fields
   * @param name name
   * @return true if contains
   */
  protected static boolean notContainsFields(List<RowType.RowField> fields, String name) {
    return fields.stream().noneMatch(f -> f.getName().equals(name));
  }

  /**
   * add field to fields.
   * @param fields fields
   * @param name name
   * @param type type
   */
  public void addField(List<RowType.RowField> fields, String name, LogicalType type) {
    if (notContainsFields(fields, name)) {
      fields.add(new RowType.RowField(name, type));
    } else {
      for (int i = 0; i < fields.size(); i++) {
        final var fieldName = name + (i + 1);
        if (notContainsFields(fields, fieldName)) {
          fields.add(new RowType.RowField(fieldName, type));
          break;
        }
      }
    }
  }

  @Override
  public FlatMapFunction<RowData, RowData> createFlatMap(TransformContext context) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public TypeInformation<RowData> returns() {
    throw new UnsupportedOperationException("Not implemented");
  }
}
