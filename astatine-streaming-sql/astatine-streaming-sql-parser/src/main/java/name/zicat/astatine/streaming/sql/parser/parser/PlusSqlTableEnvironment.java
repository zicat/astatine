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

package name.zicat.astatine.streaming.sql.parser.parser;

import static name.zicat.astatine.streaming.sql.parser.transform.TransformFactory.findFactory;
import static name.zicat.astatine.streaming.sql.parser.utils.ViewUtils.table2Stream;

import static org.apache.flink.table.catalog.SchemaTranslator.createConsumingResult;

import name.zicat.astatine.streaming.sql.parser.transform.OneTransformFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformContext;
import name.zicat.astatine.streaming.sql.parser.transform.TwoTransformFactory;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/** PlusSqlTableEnvironment. */
public class PlusSqlTableEnvironment {

  private static final Pattern WATERMARK_PATTERN = Pattern.compile("WATERMARK FOR (.*) AS (.*)");
  public static final ConfigOption<String> OPTION_WATERMARK_EXPRESSION =
      ConfigOptions.key("expression.watermark").stringType().defaultValue(null);
  public static final ConfigOption<Integer> OPTION_PARALLELISM =
      ConfigOptions.key("parallelism").intType().defaultValue(-1);
  private static final ConfigOption<String> OPTION_PRODUCT_TYPE =
      ConfigOptions.key("product.type").stringType().defaultValue(Row.class.getName());

  protected final StreamTableEnvironmentImpl tEnv;
  protected final PlusSqlParser plusSqlParser = new PlusSqlParser();
  protected final Map<String, DataStream<?>> streamCache = new HashMap<>();

  public PlusSqlTableEnvironment(StreamTableEnvironmentImpl tEnv) {
    this.tEnv = tEnv;
  }

  /**
   * register sql with statement set.
   *
   * @param statementSet statementSet
   * @param sql sql
   * @return return true if register success
   */
  public boolean executeSql(StatementSet statementSet, String sql) throws ClassNotFoundException {
    final var streamOperation = plusSqlParser.parse(sql);
    final var source = streamOperation.source();
    final var name = streamOperation.name();
    final var operationType = streamOperation.streamType();
    final var operatorOperations = streamOperation.streamOperatorOperations();
    final var configuration = tEnv.getConfig().getConfiguration();

    if (streamCache.containsKey(name)) {
      throw new RuntimeException(name + " already exists");
    }

    final var sourceConfig = new Configuration(configuration);
    sourceConfig.addAll(Configuration.fromMap(streamOperation.sourceProperties()));

    var stream = getStream(source, sourceConfig);

    for (var operatorOperation : operatorOperations) {
      final var config = new Configuration(configuration);
      config.addAll(Configuration.fromMap(operatorOperation.properties()));
      final var context = new TransformContext(tEnv, config);

      final var operatorSource = operatorOperation.source();
      final var type = operatorOperation.type();
      if (operatorSource == null) {
        stream = findFactory(type).transform(context, stream);
      } else {
        final var stream2 = getStream(operatorSource, context);
        final var newType =
            OneTransformFactory.IDENTITY.equals(type) ? TwoTransformFactory.IDENTITY : type;
        stream = findFactory(newType).transform(context, stream, stream2);
      }
      final var parallelism = context.get(OPTION_PARALLELISM);
      if (parallelism > 0) {
        if (stream instanceof SingleOutputStreamOperator<?> singleOutputStream) {
          stream = singleOutputStream.setParallelism(parallelism);
        } else {
          throw new UnsupportedOperationException(
              "parallelism only support "
                  + SingleOutputStreamOperator.class
                  + ", not support on "
                  + stream.getClass());
        }
      }
    }

    final var nameConfig = new Configuration(configuration);
    nameConfig.addAll(Configuration.fromMap(streamOperation.nameProperties()));

    if (operationType == StreamType.CREATE_STREAM) {
      streamCache.put(name, stream);
      return true;
    } else if (operationType == StreamType.PRINT_FROM) {
      final var print = TableDescriptor.forConnector("print").build();
      statementSet.add(tEnv.fromDataStream(stream).insertInto(print));
      return true;
    } else if (operationType == StreamType.CREATE_VIEW) {
      final var watermarkExpression = nameConfig.getString(OPTION_WATERMARK_EXPRESSION);
      if (watermarkExpression == null) {
        tEnv.createTemporaryView(name, stream);
        return true;
      }
      final var streamType = stream.getType();
      final var typeFactory = tEnv.getCatalogManager().getDataTypeFactory();
      final var consumeResult = createConsumingResult(typeFactory, streamType, null);
      final var schema = consumeResult.getSchema();
      final var schemaBuilder =
          assignWatermarkExpression(Schema.newBuilder().fromSchema(schema), watermarkExpression);
      tEnv.createTemporaryView(name, stream, schemaBuilder.build());
      return true;
    } else {
      return false;
    }
  }

  /**
   * get stream, first from cache, then from tEnv.
   *
   * @param source source
   * @return DataStream
   */
  private DataStream<?> getStream(String source, ReadableConfig config)
      throws ClassNotFoundException {
    final var streamInCache = streamCache.get(source);
    DataStream<?> stream;
    if (streamInCache == null) {
      final var productType = config.get(OPTION_PRODUCT_TYPE);
      final var productTypeClass =
          "RowData".equals(productType)
              ? RowData.class
              : Class.forName(config.get(OPTION_PRODUCT_TYPE));
      final var table = tEnv.from(source);
      stream = table2Stream(tEnv, table, productTypeClass);
    } else {
      stream = streamInCache;
    }
    if (stream == null) {
      throw new RuntimeException("source not found " + source);
    }
    return stream;
  }

  public StreamTableEnvironmentImpl streamTableEnvironment() {
    return tEnv;
  }

  public Map<String, DataStream<?>> streamCache() {
    return streamCache;
  }

  public static Schema.Builder assignWatermarkExpression(
      Schema.Builder builder, String expression) {
    expression = expression.trim();
    final var matcher = WATERMARK_PATTERN.matcher(expression.trim());
    if (!matcher.find()) {
      throw new IllegalArgumentException(OPTION_WATERMARK_EXPRESSION.key() + " parse error");
    }
    return builder.watermark(matcher.group(1), matcher.group(2));
  }
}
