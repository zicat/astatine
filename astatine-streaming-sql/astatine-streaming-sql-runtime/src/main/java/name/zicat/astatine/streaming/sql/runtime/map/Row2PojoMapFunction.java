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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.runtime.generated.CompileUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;

/** Row2PojoMapFunction. */
public class Row2PojoMapFunction<T> extends RichMapFunction<Row, T> {

    private static final Logger LOG = LoggerFactory.getLogger(Row2PojoMapFunction.class);
    private static final AtomicInteger COUNTER = new AtomicInteger(0);

    private final RowType rowType;
    private final String pojoClassName;
    private transient MapFunction<Row, T> mapFunction;

    public Row2PojoMapFunction(RowType rowType, String pojoClassName) {
        this.rowType = rowType;
        this.pojoClassName = pojoClassName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        final var functionClassName = getClass().getSimpleName() + COUNTER.incrementAndGet();
        final var mapFunctionCode =
                buildMapFunctionCode(rowType, Class.forName(pojoClassName), functionClassName);
        final var classLoader = getRuntimeContext().getUserCodeClassLoader();
        final Class<MapFunction<Row, T>> dynamicClass =
                CompileUtils.compile(classLoader, functionClassName, mapFunctionCode);
        mapFunction = dynamicClass.getDeclaredConstructor().newInstance();
    }

    @Override
    public T map(Row row) throws Exception {
        return mapFunction.map(row);
    }

    /**
     * row 2 pojo set.
     *
     * @param rowType rowType
     * @param pojoClass pojoClass
     * @return string
     */
    private static String buildMapFunctionCode(
            RowType rowType, Class<?> pojoClass, String functionClassName) {

        final var line = System.lineSeparator();
        final var pojoName = pojoClass.getName();

        final var codeBuilder = new StringBuilder();
        codeBuilder.append("import org.apache.flink.api.common.functions.MapFunction;");
        codeBuilder.append(line);
        codeBuilder.append("import org.apache.flink.types.Row;");
        codeBuilder.append(line);
        codeBuilder.append("import java.lang.Object;");
        codeBuilder.append(line).append(line);

        codeBuilder.append("public class %s implements MapFunction {".formatted(functionClassName));
        codeBuilder.append(line).append(line);
        codeBuilder.append("    public Object map(Object obj) throws java.lang.Exception {");
        codeBuilder.append(line).append(line);
        codeBuilder.append("        final Row row = (Row) obj;");
        codeBuilder.append(line);
        codeBuilder.append("        final %s result = new %s();".formatted(pojoName, pojoName));
        codeBuilder.append(line);
        row2PojoSetBuild("        ", codeBuilder, rowType, pojoClass);
        codeBuilder.append(line);
        codeBuilder.append("        return result;").append(line);
        codeBuilder.append("    }").append(line);
        codeBuilder.append("}");
        return codeBuilder.toString();
    }

    /**
     * row 2 pojo set code.
     *
     * @param codeBuilder codeBuilder
     * @param rowType rowType
     * @param pojoName pojoName
     */
    private static void row2PojoSetBuild(
            final String prefix, StringBuilder codeBuilder, RowType rowType, Class<?> pojoName) {
        if (pojoName.equals(Object.class)) {
            return;
        }
        // build fields in super class first.
        row2PojoSetBuild(prefix, codeBuilder, rowType, pojoName.getSuperclass());

        for (Field field : pojoName.getDeclaredFields()) {
            final var fieldName = getColumnName(field);
            final var index = rowType.getFieldIndex(fieldName);
            if (index == -1) {
                LOG.warn("field {} not found in row type {}", fieldName, rowType);
                continue;
            }
            var getRowValueCode = getRowValueCode(field, index);
            codeBuilder
                    .append(prefix)
                    .append("result.")
                    .append(setMethodName(field))
                    .append("((")
                    .append(fieldType(field))
                    .append(")")
                    .append(getRowValueCode)
                    .append(");");
            codeBuilder.append(System.lineSeparator());
        }
    }

    private static String getRowValueCode(Field field, int index) {
        var getRowValueCode = "row.getFieldAs(" + index + ")";
        final var row2PojoProperty = field.getAnnotation(Row2PojoProperty.class);
        if (row2PojoProperty != null && !row2PojoProperty.convertCode().isEmpty()) {
            final var convertCode = row2PojoProperty.convertCode();
            if (!convertCode.contains("{column}")) {
                throw new RuntimeException("convert code must contain {column}");
            }
            getRowValueCode = convertCode.replace("{column}", getRowValueCode);
        }
        return getRowValueCode;
    }

    /**
     * get column name, if set Row2PojoProperty, use it, else use field name.
     *
     * @param field field
     * @return name
     */
    private static String getColumnName(Field field) {
        final var row2PojoProperty = field.getAnnotation(Row2PojoProperty.class);
        return row2PojoProperty == null || row2PojoProperty.value().isEmpty()
                ? field.getName()
                : row2PojoProperty.value();
    }

    /**
     * set field method.
     *
     * @param field field
     * @return String
     */
    private static String setMethodName(Field field) {
        final var fieldName = field.getName();
        final var fieldNameFormat =
                fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        return "set" + fieldNameFormat;
    }

    /**
     * convert all basic type to wrapper type.
     *
     * @param field field
     * @return String
     */
    private static String fieldType(Field field) {
        final var type = field.getType();
        return switch (type.getName()) {
            case "int" -> Integer.class.getName();
            case "boolean" -> Boolean.class.getName();
            case "byte" -> Byte.class.getName();
            case "short" -> Short.class.getName();
            case "long" -> Long.class.getName();
            case "float" -> Float.class.getName();
            case "double" -> Double.class.getName();
            case "char" -> Character.class.getName();
            case "[B" -> "byte []";
            default -> type.getName();
        };
    }
}
