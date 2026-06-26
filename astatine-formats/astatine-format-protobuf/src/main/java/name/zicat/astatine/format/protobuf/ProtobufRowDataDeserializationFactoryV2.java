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

package name.zicat.astatine.format.protobuf;

import java.util.*;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.protobuf.PbEncodingFormat;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.formats.protobuf.PbFormatOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

/** ProtobufRowDataDeserializationFactoryV2. */
public class ProtobufRowDataDeserializationFactoryV2
    implements SerializationFormatFactory, DeserializationFormatFactory {

  public static final ConfigOption<String> OPTION_MESSAGE_CLASS_NAME =
      ConfigOptions.key("message-class-name").stringType().noDefaultValue();
  public static final ConfigOption<Boolean> OPTION_IGNORE_PARSE_ERROR =
      ConfigOptions.key("ignore-parse-errors").booleanType().defaultValue(false);

  @Override
  public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
      DynamicTableFactory.Context context, ReadableConfig readableConfig) {

    return new DecodingFormat<>() {
      @Override
      public DeserializationSchema<RowData> createRuntimeDecoder(
          DynamicTableSource.Context context, DataType producedDataType) {
        final RowType rowType = (RowType) producedDataType.getLogicalType();
        final TypeInformation<RowData> rowDataTypeInfo =
            context.createTypeInformation(producedDataType);
        return createDeserializationSchema(rowType, rowDataTypeInfo, readableConfig);
      }

      @Override
      public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
      }
    };
  }

  private DeserializationSchema<RowData> createDeserializationSchema(
      RowType rowType, TypeInformation<RowData> rowDataTypeInfo, ReadableConfig readableConfig) {
    final boolean ignoreParseErrors = readableConfig.get(OPTION_IGNORE_PARSE_ERROR);
    final String messageClassName = readableConfig.get(OPTION_MESSAGE_CLASS_NAME);
    return new ProtobufRowDataDeserializationSchemaV2(
        ignoreParseErrors, rowType, rowDataTypeInfo, messageClassName);
  }

  @Override
  public String factoryIdentifier() {
    return "protobuf_v2";
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    final Set<ConfigOption<?>> set = new HashSet<>();
    set.add(OPTION_MESSAGE_CLASS_NAME);
    set.add(OPTION_IGNORE_PARSE_ERROR);
    return set;
  }

  @Override
  public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
      DynamicTableFactory.Context context, ReadableConfig formatOptions) {
    // protobuf_v2 using flink protobuf to support encoding format
    FactoryUtil.validateFactoryOptions(this, formatOptions);
    return new PbEncodingFormat(buildConfig(formatOptions));
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    Set<ConfigOption<?>> result = new HashSet<>();
    result.add(PbFormatOptions.MESSAGE_CLASS_NAME);
    return result;
  }

  private static PbFormatConfig buildConfig(ReadableConfig formatOptions) {
    PbFormatConfig.PbFormatConfigBuilder configBuilder = new PbFormatConfig.PbFormatConfigBuilder();
    configBuilder.messageClassName(formatOptions.get(PbFormatOptions.MESSAGE_CLASS_NAME));
    formatOptions
        .getOptional(PbFormatOptions.IGNORE_PARSE_ERRORS)
        .ifPresent(configBuilder::ignoreParseErrors);
    formatOptions
        .getOptional(PbFormatOptions.READ_DEFAULT_VALUES)
        .ifPresent(configBuilder::readDefaultValues);
    formatOptions
        .getOptional(PbFormatOptions.WRITE_NULL_STRING_LITERAL)
        .ifPresent(configBuilder::writeNullStringLiterals);
    return configBuilder.build();
  }
}
