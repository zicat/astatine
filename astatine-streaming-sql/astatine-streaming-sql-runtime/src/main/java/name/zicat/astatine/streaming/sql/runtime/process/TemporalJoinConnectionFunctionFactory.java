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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataTypeQueryable;
import org.apache.flink.table.types.logical.RowType;

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
    final var leftType = (DataTypeQueryable) connectedStream.getFirstInput().getType();
    final var rightType = (DataTypeQueryable) connectedStream.getSecondInput().getType();

    final var joinType = context.get(OPTION_JOIN_TYPE);
    final var minRetentionTime =
        context.get(ExecutionConfigOptions.IDLE_STATE_RETENTION).toMillis();
    final var maxRetentionTime = minRetentionTime * 3 / 2;
    final var leftEventTimeIndex = fieldIndex(leftType, context.get(OPTION_LEFT_EVENTTIME));
    final var rightEventTimeIndex = fieldIndex(rightType, context.get(OPTION_RIGHT_EVENTTIME));
    final var leftRowType = (RowType) leftType.getDataType().getLogicalType();
    final var rightRowType = (RowType) rightType.getDataType().getLogicalType();
    final var leftReturnRowIndexes =
        fieldsIndex(leftRowType, context.get(OPTION_LEFT_SELECT_FIELDS));
    final var rightReturnRowIndexes =
        fieldsIndex(rightRowType, context.get(OPTION_RIGHT_SELECT_FIELDS));
    final var leftReturnFields = rowFields(leftRowType, leftReturnRowIndexes);
    final var rightReturnFields =
        joinType == JoinType.LEFT
            ? rowFieldsNullable(rightRowType, rightReturnRowIndexes)
            : rowFields(rightRowType, rightReturnRowIndexes);

    final var returnFields =
        Stream.concat(leftReturnFields.stream(), rightReturnFields.stream()).toList();
    final var result =
        connectedStream.process(
            createTemporalJoinConnectionFunction(
                InternalTypeInfo.of(new RowType(leftReturnFields)),
                InternalTypeInfo.of(new RowType(rightReturnFields)),
                leftEventTimeIndex,
                rightEventTimeIndex,
                minRetentionTime,
                maxRetentionTime,
                joinType,
                leftReturnRowIndexes,
                rightReturnRowIndexes));
    return result
        .name(identity() + "_" + result.getId())
        .returns(InternalTypeInfo.of(new RowType(returnFields)));
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
