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

package name.zicat.astatine.format.protobuf.serialize;

import com.google.protobuf.Descriptors;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.formats.protobuf.PbCodegenException;
import org.apache.flink.formats.protobuf.PbFormatContext;
import org.apache.flink.formats.protobuf.serialize.PbCodegenSerializeFactory;
import org.apache.flink.formats.protobuf.serialize.PbCodegenSerializer;
import org.apache.flink.formats.protobuf.util.PbCodegenAppender;
import org.apache.flink.formats.protobuf.util.PbCodegenVarId;
import org.apache.flink.formats.protobuf.util.PbFormatUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.codehaus.janino.SimpleCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** PbCodegenUtilsV2. */
@SuppressWarnings("rawtypes")
public class PbCodegenUtilsV2 {
  private static final Logger LOG = LoggerFactory.getLogger(PbCodegenUtilsV2.class);

  public static String flinkContainerElementCode(
      String flinkContainerCode, String index, LogicalType eleType) {
    return switch (eleType.getTypeRoot()) {
      case INTEGER -> flinkContainerCode + ".getInt(" + index + ")";
      case BIGINT -> flinkContainerCode + ".getLong(" + index + ")";
      case FLOAT -> flinkContainerCode + ".getFloat(" + index + ")";
      case DOUBLE -> flinkContainerCode + ".getDouble(" + index + ")";
      case BOOLEAN -> flinkContainerCode + ".getBoolean(" + index + ")";
      case VARCHAR, CHAR -> flinkContainerCode + ".getString(" + index + ")";
      case VARBINARY, BINARY -> flinkContainerCode + ".getBinary(" + index + ")";
      case ROW -> {
        int size = eleType.getChildren().size();
        yield flinkContainerCode + ".getRow(" + index + ", " + size + ")";
      }
      case MAP -> flinkContainerCode + ".getMap(" + index + ")";
      case ARRAY -> flinkContainerCode + ".getArray(" + index + ")";
      default ->
          throw new IllegalArgumentException(
              "Unsupported data type in schema: " + String.valueOf(eleType));
    };
  }

  public static String getTypeStrFromProto(Descriptors.FieldDescriptor fd, boolean isList)
      throws PbCodegenException {
    String typeStr;
    switch (fd.getJavaType()) {
      case MESSAGE:
        if (fd.isMapField()) {
          Descriptors.FieldDescriptor keyFd = fd.getMessageType().findFieldByName("key");
          Descriptors.FieldDescriptor valueFd = fd.getMessageType().findFieldByName("value");
          String keyTypeStr = getTypeStrFromProto(keyFd, false);
          String valueTypeStr = getTypeStrFromProto(valueFd, false);
          typeStr = "Map<" + keyTypeStr + "," + valueTypeStr + ">";
        } else {
          typeStr = PbFormatUtils.getFullJavaName(fd.getMessageType());
        }
        break;
      case INT:
        typeStr = "Integer";
        break;
      case LONG:
        typeStr = "Long";
        break;
      case STRING:
        typeStr = "String";
        break;
      case ENUM:
        typeStr = PbFormatUtils.getFullJavaName(fd.getEnumType());
        break;
      case FLOAT:
        typeStr = "Float";
        break;
      case DOUBLE:
        typeStr = "Double";
        break;
      case BYTE_STRING:
        typeStr = "ByteString";
        break;
      case BOOLEAN:
        typeStr = "Boolean";
        break;
      default:
        throw new PbCodegenException(
            "do not support field type: " + String.valueOf(fd.getJavaType()));
    }

    return isList ? "List<" + typeStr + ">" : typeStr;
  }

  public static String getTypeStrFromLogicType(LogicalType type) {
    return switch (type.getTypeRoot()) {
      case INTEGER -> "int";
      case BIGINT -> "long";
      case FLOAT -> "float";
      case DOUBLE -> "double";
      case BOOLEAN -> "boolean";
      case VARCHAR, CHAR -> "StringData";
      case VARBINARY, BINARY -> "byte[]";
      case ROW -> "RowData";
      case MAP -> "MapData";
      case ARRAY -> "ArrayData";
      default ->
          throw new IllegalArgumentException(
              "Unsupported data type in schema: " + String.valueOf(type));
    };
  }

  public static String pbDefaultValueCode(
      Descriptors.FieldDescriptor fieldDescriptor, PbFormatContext pbFormatContext)
      throws PbCodegenException {
    String nullLiteral = pbFormatContext.getPbFormatConfig().getWriteNullStringLiterals();
    switch (fieldDescriptor.getJavaType()) {
      case MESSAGE -> {
        return PbFormatUtils.getFullJavaName(fieldDescriptor.getMessageType())
            + ".getDefaultInstance()";
      }
      case INT -> {
        return "0";
      }
      case LONG -> {
        return "0L";
      }
      case STRING -> {
        return "\"" + nullLiteral + "\"";
      }
      case ENUM -> {
        return PbFormatUtils.getFullJavaName(fieldDescriptor.getEnumType()) + ".values()[0]";
      }
      case FLOAT -> {
        return "0.0f";
      }
      case DOUBLE -> {
        return "0.0d";
      }
      case BYTE_STRING -> {
        return "ByteString.EMPTY";
      }
      case BOOLEAN -> {
        return "false";
      }
      default ->
          throw new PbCodegenException(
              "do not support field type: " + String.valueOf(fieldDescriptor.getJavaType()));
    }
  }

  public static String convertFlinkArrayElementToPbWithDefaultValueCode(
      String flinkArrDataVar,
      String iVar,
      String resultPbVar,
      Descriptors.FieldDescriptor elementPbFd,
      LogicalType elementDataType,
      PbFormatContext pbFormatContext,
      int indent)
      throws PbCodegenException {
    PbCodegenVarId varUid = PbCodegenVarId.getInstance();
    int uid = varUid.getAndIncrement();
    String flinkElementVar = "elementVar" + uid;
    PbCodegenAppender appender = new PbCodegenAppender(indent);
    String protoTypeStr = getTypeStrFromProto(elementPbFd, false);
    String dataTypeStr = getTypeStrFromLogicType(elementDataType);
    appender.appendLine(protoTypeStr + " " + resultPbVar);
    appender.begin("if(" + flinkArrDataVar + ".isNullAt(" + iVar + ")){");
    appender.appendLine(resultPbVar + "=" + pbDefaultValueCode(elementPbFd, pbFormatContext));
    appender.end("}else{");
    appender.begin();
    appender.appendLine(dataTypeStr + " " + flinkElementVar);
    String flinkContainerElementCode =
        flinkContainerElementCode(flinkArrDataVar, iVar, elementDataType);
    appender.appendLine(flinkElementVar + " = " + flinkContainerElementCode);
    PbCodegenSerializer codegenSer =
        PbCodegenSerializeFactory.getPbCodegenSer(elementPbFd, elementDataType, pbFormatContext);
    String code = codegenSer.codegen(resultPbVar, flinkElementVar, appender.currentIndent());
    appender.appendSegment(code);
    appender.end("}");
    return appender.code();
  }

  public static Class compileClass(ClassLoader classloader, String className, String code)
      throws ClassNotFoundException {
    SimpleCompiler simpleCompiler = new SimpleCompiler();
    simpleCompiler.setParentClassLoader(classloader);

    try {
      simpleCompiler.cook(code);
    } catch (Throwable t) {
      LOG.error("Protobuf codegen compile error: \n{}", code);
      throw new InvalidProgramException(
          "Program cannot be compiled. This is a bug. Please file an issue.", t);
    }

    return simpleCompiler.getClassLoader().loadClass(className);
  }

  public static boolean needToSplit(int noSplitCodeSize) {
    return noSplitCodeSize >= 4000;
  }
}
