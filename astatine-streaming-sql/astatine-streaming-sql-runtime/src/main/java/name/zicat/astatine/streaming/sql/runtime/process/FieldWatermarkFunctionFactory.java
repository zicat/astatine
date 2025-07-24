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

import name.zicat.astatine.streaming.sql.parser.function.WatermarkFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformContext;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataTypeQueryable;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import java.io.Serializable;
import java.time.Duration;

import static name.zicat.astatine.streaming.sql.parser.utils.Types.fieldsNameTypes;

/** FieldWatermarkFunctionFactory. */
public class FieldWatermarkFunctionFactory implements WatermarkFunctionFactory<RowData> {

  public static final String IDENTITY = "field_watermark";

  public static final ConfigOption<String> OPTION_FIELD =
      ConfigOptions.key("field").stringType().noDefaultValue();
  public static final ConfigOption<Duration> OPTION_DELAY_DURATION =
      ConfigOptions.key("delay").durationType().noDefaultValue();
  public static final ConfigOption<Boolean> OPTION_EMIT_ON_EVENT =
      ConfigOptions.key("emit.on-event").booleanType().defaultValue(false);

  @Override
  public DataStream<RowData> transform(TransformContext context, DataStream<RowData> stream) {
    final var type = (DataTypeQueryable) stream.getType();
    final var rowType = (RowType) type.getDataType().getLogicalType();
    final var fieldName = context.get(OPTION_FIELD);
    if (fieldName == null) {
      throw new IllegalArgumentException("Field name must be specified using the 'field' option.");
    }
    final var fieldNameType = fieldsNameTypes(rowType, fieldName)[0];
    final var emitOnEvent = context.get(OPTION_EMIT_ON_EVENT);
    if (!(fieldNameType.getType() instanceof TimestampType)) {
      throw new IllegalArgumentException("param field not set");
    }
    final var getter = fieldNameType.fieldGetter();
    final var delayMill = context.get(OPTION_DELAY_DURATION).toMillis();
    return stream.assignTimestampsAndWatermarks(
        new SerializableWatermarkStrategy(
            new FieldDelayWatermarkGenerator(getter, delayMill, emitOnEvent)));
  }

  @Override
  public String identity() {
    return IDENTITY;
  }

  @Override
  public WatermarkStrategy<RowData> createWatermarkStrategy(TransformContext context) {
    throw new UnsupportedOperationException("not support");
  }

  /** SerializableWatermarkStrategy. */
  private static class SerializableWatermarkStrategy
      implements WatermarkStrategy<RowData>, Serializable {

    private final WatermarkGenerator<RowData> generator;

    public SerializableWatermarkStrategy(WatermarkGenerator<RowData> generator) {
      this.generator = generator;
    }

    @Override
    public WatermarkGenerator<RowData> createWatermarkGenerator(
        WatermarkGeneratorSupplier.Context context) {
      return generator;
    }
  }

  /** FieldDelayWatermarkGenerator. */
  private static class FieldDelayWatermarkGenerator
      implements WatermarkGenerator<RowData>, Serializable {

    private final RowData.FieldGetter getter;
    private final long delayMills;
    private final boolean emitOnEvent;
    private long currentWatermark = Long.MIN_VALUE;

    public FieldDelayWatermarkGenerator(
        RowData.FieldGetter getter, long delayMills, boolean emitOnEvent) {
      this.getter = getter;
      this.delayMills = delayMills;
      this.emitOnEvent = emitOnEvent;
    }

    @Override
    public void onEvent(RowData event, long eventTimestamp, WatermarkOutput output) {
      final var timestamp = (TimestampData) getter.getFieldOrNull(event);
      if (timestamp == null) {
        return;
      }
      currentWatermark = Math.max(currentWatermark, timestamp.getMillisecond() - delayMills);
      if (emitOnEvent) {
        output.emitWatermark(new Watermark(currentWatermark));
      }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
      if (emitOnEvent) {
        return;
      }
      output.emitWatermark(new Watermark(currentWatermark));
    }
  }
}
