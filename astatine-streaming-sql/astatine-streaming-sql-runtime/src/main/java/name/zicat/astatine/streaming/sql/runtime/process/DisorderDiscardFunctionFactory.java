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

package name.zicat.astatine.streaming.sql.runtime.process;

import name.zicat.astatine.streaming.sql.parser.function.KeyedProcessFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataTypeQueryable;
import org.apache.flink.table.types.logical.RowType;

import static name.zicat.astatine.streaming.sql.parser.utils.Types.fieldGetter;

/** DisorderDiscardFunctionFactory. */
public class DisorderDiscardFunctionFactory
    implements KeyedProcessFunctionFactory<RowData, RowData, RowData> {

  public static final ConfigOption<String> OPTION_EVENTTIME =
      ConfigOptions.key("eventtime").stringType().noDefaultValue();

  public static final String IDENTITY = "disorder_discard";

  @Override
  public DataStream<RowData> transform(
      TransformContext context, KeyedStream<RowData, RowData> keyedStream) {
    final var type = (DataTypeQueryable) keyedStream.getType();
    final var rowType = (RowType) type.getDataType().getLogicalType();
    final InternalTypeInfo<RowData> rowTypeInfo = InternalTypeInfo.of(rowType);
    final var eventTimeGetter = fieldGetter(rowType, context.get(OPTION_EVENTTIME));
    final var result =
        keyedStream.process(new DisorderDiscardFunction(eventTimeGetter, rowTypeInfo));
    return result.name(identity() + "_" + result.getId()).returns(keyedStream.getType());
  }

  @Override
  public String identity() {
    return IDENTITY;
  }

  @Override
  public TypeInformation<RowData> returns() {
    throw new UnsupportedOperationException();
  }

  @Override
  public KeyedProcessFunction<RowData, RowData, RowData> createKeyedProcess(
      TransformContext context) {
    throw new UnsupportedOperationException();
  }
}
