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

package name.zicat.astatine.format.protobuf.parser;

import com.google.protobuf.Descriptors;
import java.util.List;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/** PBFactory. */
public class PBFactory {

    /** create value reader by descriptor and logical type. */
    public PBValueReader createValueReader(
            Descriptors.GenericDescriptor descriptor, LogicalType logicalType) {
        if (descriptor instanceof Descriptors.FieldDescriptor fieldDescriptor) {
            if (fieldDescriptor.isMapField()) {
                return new PBMapValueReader(fieldDescriptor, logicalType, this);
            }
            return switch (fieldDescriptor.getJavaType()) {
                case INT -> new IntPBValueReader(logicalType, descriptor);
                case LONG -> new Int64PBValueReader(logicalType, descriptor);
                case STRING, BYTE_STRING -> new StringPBValueReader(logicalType, descriptor);
                case BOOLEAN -> new BoolPBValueReader(logicalType, descriptor);
                case FLOAT -> new FloatPBValueReader(logicalType, descriptor);
                case DOUBLE -> new DoublePBValueReader(logicalType, descriptor);
                case ENUM -> new EnumPBValueReader(logicalType, fieldDescriptor);
                case MESSAGE ->
                        new PBMessageValueReader(
                                logicalType, fieldDescriptor.getMessageType(), this);
            };
        } else if (descriptor instanceof Descriptors.Descriptor messageDescriptor) {
            return new PBMessageValueTopReader(logicalType, messageDescriptor, this);
        } else {
            throw new UnsupportedOperationException("unsupported descriptor " + descriptor);
        }
    }

    /** create field setter by descriptor and logical type. */
    public PBFieldSetter createFieldSetter(
            Descriptors.FieldDescriptor fieldDescriptor, LogicalType logicalType) {
        if (fieldDescriptor.isMapField()) {
            return new PBMapFieldSetter(fieldDescriptor, logicalType, this);
        }
        if (fieldDescriptor.isRepeated()) {
            return new PBRepeatedFieldSetter(fieldDescriptor, logicalType, this);
        }
        return new PBRowFieldSetter(fieldDescriptor, logicalType, this);
    }

    /** create field setter collection by protobuf fields and row type. */
    public PBFieldDispatcher createFieldDispatcher(
            List<Descriptors.FieldDescriptor> protobufFields, RowType rowType) {
        final var rowFieldNames = rowType.getFieldNames();
        final var fieldDispatcher = new PBFieldDispatcher();
        for (int rowIndex = 0; rowIndex < rowFieldNames.size(); rowIndex++) {
            final var name = rowFieldNames.get(rowIndex);
            boolean found = false;
            for (Descriptors.FieldDescriptor fieldDescriptor : protobufFields) {
                if (!fieldDescriptor.getName().equals(name)) {
                    continue;
                }
                final var logicalType = rowType.getTypeAt(rowIndex);
                fieldDispatcher.add(
                        fieldDescriptor.getNumber(),
                        rowIndex,
                        createFieldSetter(fieldDescriptor, logicalType));
                found = true;
                break;
            }
            if (!found) {
                throw new RuntimeException("name not found in pb " + name);
            }
        }
        return fieldDispatcher;
    }
}
