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

import name.zicat.astatine.streaming.sql.parser.function.MapFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformContext;
import name.zicat.astatine.streaming.sql.parser.utils.AstatineCompileUtils;
import name.zicat.astatine.streaming.sql.runtime.CalcCodeGenerator;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.CompileUtils;
import org.apache.flink.table.runtime.generated.GeneratedClass;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataTypeQueryable;
import org.apache.flink.table.types.logical.RowType;

import static name.zicat.astatine.streaming.sql.parser.utils.ViewUtils.createUniqueViewName;

/** SelectFunctionFactory. */
public class SelectMapFunctionFactory implements MapFunctionFactory<RowData, RowData> {

  public static final String IDENTITY = "select";

  public static final ConfigOption<String> OPTION_EXPRESSION =
      ConfigOptions.key("expression").stringType().noDefaultValue();

  private static final String SELECT_SQL = "SELECT %s FROM %s";
  private static final String OPERATOR_NAME = "MapSqlFunction";

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public DataStream<RowData> transform(TransformContext context, DataStream<RowData> stream) {
    final var tEnv = context.streamTableEnvironmentImpl();
    final var viewName = createUniqueViewName(tEnv.listTables());
    tEnv.createTemporaryView(viewName, tEnv.fromDataStream(stream));
    try {
      final var parser = tEnv.getParser();
      final var operators =
          parser.parse(SELECT_SQL.formatted(context.get(OPTION_EXPRESSION), viewName));
      final var inputType =
          (RowType) ((DataTypeQueryable) stream.getType()).getDataType().getLogicalType();
      final var operator = (PlannerQueryOperation) operators.get(0);
      final var outputType =
          (RowType) operator.getResolvedSchema().toSourceRowDataType().getLogicalType();
      final var classLoader = ((PlannerBase) tEnv.getPlanner()).getFlinkContext().getClassLoader();
      final var ctx = new CodeGeneratorContext(context, classLoader);
      final var logicalProject = (LogicalProject) operator.getCalciteTree();
      final var codeGenOperatorFactory =
          CalcCodeGenerator.generateMapFunction(
              ctx,
              inputType,
              outputType,
              JavaScalaConversionUtil.toScala(logicalProject.getProjects()),
              OPERATOR_NAME);
      return stream.map(
          (RichMapFunction) new MapSqlFunctionWrapper(codeGenOperatorFactory.getGeneratedClass()),
          InternalTypeInfo.of(outputType));
    } finally {
      tEnv.dropTemporaryView(viewName);
    }
  }

  @Override
  public MapFunction<RowData, RowData> createMap(TransformContext context) {
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

  /** MapSqlFunctionWrapper. */
  public static class MapSqlFunctionWrapper extends RichMapFunction<Object, Object> {

    private final GeneratedClass<?> generatedClass;
    private transient MapFunction<Object, Object> mapFunction;

    public MapSqlFunctionWrapper(GeneratedClass<?> generatedClass) {
      this.generatedClass = generatedClass;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      final var classLoader = getRuntimeContext().getUserCodeClassLoader();
      final Class<MapFunction<Object, Object>> dynamicClass =
          AstatineCompileUtils.compile(
              classLoader, generatedClass.getClassName(), generatedClass.getCode());
      mapFunction =
          dynamicClass
              .getConstructor(Object[].class)
              .newInstance(new Object[] {generatedClass.getReferences()});
    }

    @Override
    public Object map(Object o) throws Exception {
      return mapFunction.map(o);
    }
  }
}
