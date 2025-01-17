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

package name.zicat.astatine.streaming.sql.parser.transform;

import static name.zicat.astatine.streaming.sql.parser.parser.PlusSqlTableEnvironment.assignWatermarkExpression;
import static name.zicat.astatine.streaming.sql.parser.utils.ViewUtils.createUniqueViewName;
import static name.zicat.astatine.streaming.sql.parser.utils.ViewUtils.table2Stream;

import static org.apache.flink.table.catalog.SchemaTranslator.createConsumingResult;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Schema;

import java.util.regex.Pattern;

/** AGGREGATE WITH ("expression.select" = "SUM(value)", "expression.groupby" = "name"). */
public class AggregateTransformFactory extends OneKeyedTransformFactory {
  private static final String AGG_SQL_TEMPLATE = "SELECT %s FROM %s GROUP BY %s";
  private static final String CREATE_VIEW_SQL_TEMPLATE = "CREATE TEMPORARY VIEW %s AS %s";
  private static final Pattern TIME_ATTR_REGEX =
      Pattern.compile(
          "\\s*(TUMBLE|HOP|SESSION)\\s*\\(\\s*([^\\W_]+)\\s*,\\s*INTERVAL.*",
          Pattern.CASE_INSENSITIVE);

  private static final String WATERMARK_TEMPLATE = "WATERMARK FOR %s AS SOURCE_WATERMARK()";
  private static final ConfigOption<String> OPTION_EXPRESSION_SELECT =
      ConfigOptions.key("expression.select").stringType().noDefaultValue();
  private static final ConfigOption<String> OPTION_EXPRESSION_GROUP_BY =
      ConfigOptions.key("expression.groupby").stringType().noDefaultValue();

  @Override
  public DataStream<?> transform(TransformContext context, DataStream<?> stream) {
    final var tEnv = context.streamTableEnvironmentImpl();
    final var selectExpr = context.get(OPTION_EXPRESSION_SELECT);
    final var groupByExpr = context.get(OPTION_EXPRESSION_GROUP_BY);
    final var viewName = createUniqueViewName(tEnv.listTables());

    final var streamType = stream.getType();
    final var typeFactory = tEnv.getCatalogManager().getDataTypeFactory();
    final var consumeResult = createConsumingResult(typeFactory, streamType, null);
    final var schema = consumeResult.getSchema();
    final var resultView = createUniqueViewName(tEnv.listTables());
    try {
      final var aggSql = AGG_SQL_TEMPLATE.formatted(selectExpr, viewName, groupByExpr);
      final var schemaBuilder =
          assignWatermarkExpression(
              Schema.newBuilder().fromSchema(schema),
              WATERMARK_TEMPLATE.formatted(timeAttr(groupByExpr)));
      tEnv.createTemporaryView(viewName, tEnv.fromDataStream(stream, schemaBuilder.build()));
      final var createViewSql = CREATE_VIEW_SQL_TEMPLATE.formatted(resultView, aggSql);
      tEnv.executeSql(createViewSql);
      try {
        final var resultTable = tEnv.from(resultView);
        return table2Stream(tEnv, resultTable);
      } finally {
        tEnv.dropTemporaryView(resultView);
      }
    } finally {
      tEnv.dropTemporaryView(viewName);
    }
  }

  private static String timeAttr(String aggregateExpr) {
    final var matcher = TIME_ATTR_REGEX.matcher(aggregateExpr);
    if (!matcher.find()) {
      throw new RuntimeException(
          "Time Attribute not found, Invalid aggregate expression: " + aggregateExpr);
    }
    return matcher.group(2);
  }

  @Override
  public String identity() {
    return "AGGREGATE";
  }
}
