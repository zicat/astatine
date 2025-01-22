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

import name.zicat.astatine.streaming.sql.parser.function.ConnectFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformContext;
import name.zicat.astatine.streaming.sql.parser.utils.Types;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.stream.Stream;

import static name.zicat.astatine.streaming.sql.parser.utils.Types.*;

/** TemporalJoinConnectionFunctionFactory. */
public class TemporalJoinConnectionFunctionFactory
    implements ConnectFunctionFactory<RowData, RowData, RowData> {

  public static final String IDENTIFY = "temporal_join";
  public static final ConfigOption<String> OPTION_LEFT_EVENTTIME =
      ConfigOptions.key("left.eventtime").stringType().noDefaultValue();
  public static final ConfigOption<String> OPTION_RIGHT_EVENTTIME =
      ConfigOptions.key("right.eventtime").stringType().noDefaultValue();

  public static final ConfigOption<JoinType> OPTION_JOIN_TYPE =
      ConfigOptions.key("join.type").enumType(JoinType.class).defaultValue(JoinType.INNER);

  public static final ConfigOption<String> OPTION_LEFT_SELECT_FIELDS =
      ConfigOptions.key("left.output.fields").stringType().noDefaultValue();
  public static final ConfigOption<String> OPTION_RIGHT_SELECT_FIELDS =
      ConfigOptions.key("right.output.fields").stringType().noDefaultValue();

  @Override
  public DataStream<RowData> createConnect(
      TransformContext context, ConnectedStreams<RowData, RowData> connectedStream) {

    final var lRowType = Types.toRowType(connectedStream.getFirstInput().getType());
    final var rRowType = Types.toRowType(connectedStream.getSecondInput().getType());

    final var joinType = context.get(OPTION_JOIN_TYPE);
    final var minRetentionTime =
        context.get(ExecutionConfigOptions.IDLE_STATE_RETENTION).toMillis();
    final var maxRetentionTime = minRetentionTime * 3 / 2;

    final var lReturnFieldNameTypes =
        fieldsNameTypes(lRowType, context.get(OPTION_LEFT_SELECT_FIELDS));
    final var rReturnFieldNameTypes =
        fieldsNameTypes(rRowType, context.get(OPTION_RIGHT_SELECT_FIELDS));

    final var lReturnFields =
        Arrays.stream(lReturnFieldNameTypes).map(FieldNameType::targetRowField).toList();
    final var rReturnFields =
        joinType == JoinType.LEFT
            ? Arrays.stream(rReturnFieldNameTypes)
                .map(FieldNameType::targetNullableRowField)
                .toList()
            : Arrays.stream(rReturnFieldNameTypes).map(FieldNameType::targetRowField).toList();
    final var result =
        connectedStream.process(
            createTemporalJoinConnectionFunction(
                InternalTypeInfo.of(new RowType(lReturnFields)),
                InternalTypeInfo.of(new RowType(rReturnFields)),
                fieldNameType(lRowType, context.get(OPTION_LEFT_EVENTTIME)).getIndex(),
                fieldNameType(rRowType, context.get(OPTION_RIGHT_EVENTTIME)).getIndex(),
                minRetentionTime,
                maxRetentionTime,
                joinType,
                Arrays.stream(lReturnFieldNameTypes).mapToInt(FieldNameType::getIndex).toArray(),
                Arrays.stream(rReturnFieldNameTypes).mapToInt(FieldNameType::getIndex).toArray()));
    return result
        .name(identity() + "_" + result.getId())
        .returns(
            InternalTypeInfo.of(
                new RowType(
                    Stream.concat(lReturnFields.stream(), rReturnFields.stream()).toList())));
  }

  protected TemporalJoinConnectionFunction<?> createTemporalJoinConnectionFunction(
      InternalTypeInfo<RowData> leftTypeInfo,
      InternalTypeInfo<RowData> rightTypeInfo,
      int leftEventTimeIndex,
      int rightEventTimeIndex,
      long minRetentionTime,
      long maxRetentionTime,
      JoinType leftJoin,
      int[] leftReturnIndexMapping,
      int[] rightReturnIndexMapping) {
    return new TemporalJoinConnectionFunction<>(
        leftTypeInfo,
        rightTypeInfo,
        leftEventTimeIndex,
        rightEventTimeIndex,
        minRetentionTime,
        maxRetentionTime,
        leftJoin,
        leftReturnIndexMapping,
        rightReturnIndexMapping);
  }

  @Override
  public String identity() {
    return IDENTIFY;
  }

  public enum JoinType {
    INNER,
    LEFT
  }
}
