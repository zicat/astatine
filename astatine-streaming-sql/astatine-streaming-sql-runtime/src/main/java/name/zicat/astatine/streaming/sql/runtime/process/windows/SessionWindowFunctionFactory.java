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

package name.zicat.astatine.streaming.sql.runtime.process.windows;

import name.zicat.astatine.streaming.sql.parser.function.KeyedProcessFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformContext;
import name.zicat.astatine.streaming.sql.parser.utils.Types;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataTypeQueryable;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;

import java.time.Duration;
import java.util.Arrays;

import static name.zicat.astatine.streaming.sql.parser.utils.Types.fieldsNameTypes;

/** SessionWindowFunctionFactory. */
public class SessionWindowFunctionFactory
    implements KeyedProcessFunctionFactory<RowData, RowData, RowData> {

  public static final String IDENTITY = "session_window";

  public static final ConfigOption<String> OPTION_FIELDS =
      ConfigOptions.key("fields").stringType().noDefaultValue();
  public static final ConfigOption<String> OPTION_EVENTTIME =
      ConfigOptions.key("eventtime").stringType().noDefaultValue();
  public static final ConfigOption<String> OPTION_VALUES =
      ConfigOptions.key("values").stringType().defaultValue(null);
  public static final ConfigOption<String> OPTION_TIME_SERIES_NAME =
      ConfigOptions.key("time-series.name").stringType().defaultValue("time_series");
  public static final ConfigOption<Duration> OPTION_SESSION_DURATION =
      ConfigOptions.key("session.duration").durationType().noDefaultValue();

  @Override
  public DataStream<RowData> transform(
      TransformContext context, KeyedStream<RowData, RowData> keyedStream) {
    final var type = (DataTypeQueryable) keyedStream.getType();
    final var rowType = (RowType) type.getDataType().getLogicalType();
    final var fieldNameTypes = fieldsNameTypes(rowType, context.get(OPTION_FIELDS));
    final var valueNameTypes = fieldsNameTypes(rowType, context.get(OPTION_VALUES));
    Arrays.stream(valueNameTypes)
        .forEach(
            t -> {
              if (!(t.getType() instanceof BigIntType)) {
                throw new IllegalArgumentException("value type must be bigint");
              }
            });
    final var eventtimeType = fieldsNameTypes(rowType, context.get(OPTION_EVENTTIME))[0];
    final var function =
        new SessionWindowFunction(
            eventtimeType.fieldGetter(),
            eventtimeType.targetRowField(),
            Arrays.stream(fieldNameTypes).mapToInt(Types.FieldNameType::getIndex).toArray(),
            Arrays.stream(fieldNameTypes)
                .map(Types.FieldNameType::targetNullableRowField)
                .toList()
                .toArray(new RowType.RowField[] {}),
            Arrays.stream(valueNameTypes).mapToInt(Types.FieldNameType::getIndex).toArray(),
            Arrays.stream(valueNameTypes)
                .map(Types.FieldNameType::targetNullableRowField)
                .toList()
                .toArray(new RowType.RowField[] {}),
            context.get(OPTION_TIME_SERIES_NAME),
            context.get(OPTION_SESSION_DURATION).toMillis());
    return keyedStream
        .process(function)
        .returns(InternalTypeInfo.of(function.returnRowType()))
        .name(IDENTITY);
  }

  @Override
  public KeyedProcessFunction<RowData, RowData, RowData> createKeyedProcess(
      TransformContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypeInformation<RowData> returns() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String identity() {
    return IDENTITY;
  }
}
