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
import org.apache.flink.formats.protobuf.PbCodegenException;
import org.apache.flink.formats.protobuf.PbFormatContext;
import org.apache.flink.formats.protobuf.serialize.PbCodegenSerializeFactory;
import org.apache.flink.formats.protobuf.serialize.PbCodegenSerializer;
import org.apache.flink.formats.protobuf.util.PbCodegenAppender;
import org.apache.flink.formats.protobuf.util.PbCodegenVarId;
import org.apache.flink.formats.protobuf.util.PbFormatUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

/** PbCodegenRowSerializerV2. */
public class PbCodegenRowSerializerV2 implements PbCodegenSerializer {

  private final Descriptors.Descriptor descriptor;
  private final RowType rowType;
  private final PbFormatContext formatContext;

  public PbCodegenRowSerializerV2(
      Descriptors.Descriptor descriptor, RowType rowType, PbFormatContext formatContext) {
    this.rowType = rowType;
    this.descriptor = descriptor;
    this.formatContext = formatContext;
  }

  public String codegen(String resultVar, String flinkObjectCode, int indent)
      throws PbCodegenException {
    PbCodegenVarId varUid = PbCodegenVarId.getInstance();
    int uid = varUid.getAndIncrement();
    PbCodegenAppender appender = new PbCodegenAppender(indent);
    String flinkRowDataVar = "rowData" + uid;
    String pbMessageTypeStr = PbFormatUtils.getFullJavaName(this.descriptor);
    String messageBuilderVar = "messageBuilder" + uid;
    appender.appendLine("RowData " + flinkRowDataVar + " = " + flinkObjectCode);
    appender.appendLine(
        pbMessageTypeStr
            + ".Builder "
            + messageBuilderVar
            + " = "
            + pbMessageTypeStr
            + ".newBuilder()");
    int index = 0;
    PbCodegenAppender splitAppender = new PbCodegenAppender(indent);
    for (String fieldName : this.rowType.getFieldNames()) {
      Descriptors.FieldDescriptor elementFd = this.descriptor.findFieldByName(fieldName);
      LogicalType subType = this.rowType.getTypeAt(this.rowType.getFieldIndex(fieldName));
      int subUid = varUid.getAndIncrement();
      String elementPbVar = "elementPbVar" + subUid;
      String elementPbTypeStr;
      if (elementFd.isMapField()) {
        elementPbTypeStr = PbCodegenUtilsV2.getTypeStrFromProto(elementFd, false);
      } else {
        elementPbTypeStr =
            PbCodegenUtilsV2.getTypeStrFromProto(elementFd, PbFormatUtils.isArrayType(subType));
      }
      String strongCamelFieldName = PbFormatUtils.getStrongCamelCaseJsonName(fieldName);
      splitAppender.begin("if(!" + flinkRowDataVar + ".isNullAt(" + index + ")){");
      boolean isSingleValueProtoString =
          elementFd.getJavaType() == Descriptors.FieldDescriptor.JavaType.STRING
              && (subType.getTypeRoot() == LogicalTypeRoot.VARCHAR
                  || subType.getTypeRoot() == LogicalTypeRoot.CHAR);
      if (isSingleValueProtoString) {
        splitAppender.appendLine(
            messageBuilderVar
                + ".set"
                + strongCamelFieldName
                + "Bytes(UncheckedUtf8ByteString.copyFrom("
                + flinkRowDataVar
                + ".getString("
                + index
                + ").toBytes()))");
        splitAppender.end("}");
        if (PbCodegenUtilsV2.needToSplit(splitAppender.code().length())) {
          String splitMethod =
              this.formatContext.splitSerializerRowTypeMethod(
                  flinkRowDataVar,
                  pbMessageTypeStr + ".Builder",
                  messageBuilderVar,
                  splitAppender.code());
          appender.appendSegment(splitMethod);
          splitAppender = new PbCodegenAppender();
        }
        ++index;
        continue;
      }
      boolean isSingleValueProtoBytes =
          elementFd.getJavaType() == Descriptors.FieldDescriptor.JavaType.BYTE_STRING
              && (subType.getTypeRoot() == LogicalTypeRoot.VARBINARY
                  || subType.getTypeRoot() == LogicalTypeRoot.BINARY);
      if (isSingleValueProtoBytes) {
        splitAppender.appendLine(
            messageBuilderVar
                + ".set"
                + strongCamelFieldName
                + "(UncheckedUtf8ByteString.copyFrom("
                + flinkRowDataVar
                + ".getBinary("
                + index
                + ")))");
        splitAppender.end("}");
        if (PbCodegenUtilsV2.needToSplit(splitAppender.code().length())) {
          String splitMethod =
              this.formatContext.splitSerializerRowTypeMethod(
                  flinkRowDataVar,
                  pbMessageTypeStr + ".Builder",
                  messageBuilderVar,
                  splitAppender.code());
          appender.appendSegment(splitMethod);
          splitAppender = new PbCodegenAppender();
        }
        ++index;
        continue;
      }

      splitAppender.appendLine(elementPbTypeStr + " " + elementPbVar);
      String flinkRowElementCode =
          PbCodegenUtilsV2.flinkContainerElementCode(flinkRowDataVar, "" + index, subType);
      PbCodegenSerializer codegen =
          PbCodegenSerializeFactory.getPbCodegenSer(elementFd, subType, this.formatContext);
      String code =
          codegen.codegen(elementPbVar, flinkRowElementCode, splitAppender.currentIndent());
      splitAppender.appendSegment(code);
      if (subType.getTypeRoot() == LogicalTypeRoot.ARRAY) {
        splitAppender.appendLine(
            messageBuilderVar + ".addAll" + strongCamelFieldName + "(" + elementPbVar + ")");
      } else if (subType.getTypeRoot() == LogicalTypeRoot.MAP) {
        splitAppender.appendLine(
            messageBuilderVar + ".putAll" + strongCamelFieldName + "(" + elementPbVar + ")");
      } else {
        splitAppender.appendLine(
            messageBuilderVar + ".set" + strongCamelFieldName + "(" + elementPbVar + ")");
      }

      splitAppender.end("}");
      if (PbCodegenUtilsV2.needToSplit(splitAppender.code().length())) {
        String splitMethod =
            this.formatContext.splitSerializerRowTypeMethod(
                flinkRowDataVar,
                pbMessageTypeStr + ".Builder",
                messageBuilderVar,
                splitAppender.code());
        appender.appendSegment(splitMethod);
        splitAppender = new PbCodegenAppender();
      }

      ++index;
    }

    if (!splitAppender.code().isEmpty()) {
      appender.appendSegment(splitAppender.code());
    }

    appender.appendLine(resultVar + " = " + messageBuilderVar + ".build()");
    return appender.code();
  }
}
