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

import java.time.Duration;

import name.zicat.astatine.streaming.sql.parser.function.KeyedProcessFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformContext;
import name.zicat.astatine.streaming.sql.parser.utils.Types;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import static name.zicat.astatine.streaming.sql.parser.utils.Types.fieldNameType;
import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.IDLE_STATE_RETENTION;

/** FieldValueWatchChangedEmitterFunctionFactory. */
public class FieldValueWatchChangedEmitterFunctionFactory
    implements KeyedProcessFunctionFactory<RowData, RowData, RowData> {

  public static final String IDENTIFY = "field_value_watch_changed_emitter";
  public static final ConfigOption<String> WATCH_FIELD =
      key("watch.field").stringType().noDefaultValue();
  public static final ConfigOption<String> EVENT_TIME_FIELD =
      key("eventtime").stringType().noDefaultValue();

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
  public DataStream<RowData> transform(
      TransformContext context, KeyedStream<RowData, RowData> keyedStream) {

    final var rowType = Types.toRowType(keyedStream.getType());
    final var retentionOption = context.getOptional(IDLE_STATE_RETENTION);
    final long minRetentionTime =
        retentionOption.map(Duration::toMillis).orElseGet(() -> Duration.ofMinutes(2).toMillis());
    final var maxRetentionTime = minRetentionTime * 3 / 2;
    final var watchFieldNameType = fieldNameType(rowType, context.get(WATCH_FIELD));
    final var returnType = InternalTypeInfo.of(rowType);
    var result =
        keyedStream.process(
            new FieldValueWatchChangedEmitterFunction<>(
                watchFieldNameType.fieldGetter(),
                watchFieldNameType.targetRowField(),
                fieldNameType(rowType, context.get(EVENT_TIME_FIELD)).getIndex(),
                minRetentionTime,
                maxRetentionTime,
                returnType));
    return result.name(identity() + "_" + result.getId()).returns(returnType);
  }

  @Override
  public String identity() {
    return IDENTIFY;
  }
}
