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

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.WireFormat;
import java.io.IOException;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.LogicalType;

/** PBRepeatedFieldSetter. */
public class PBRepeatedFieldSetter implements PBFieldSetter {

    private final Descriptors.FieldDescriptor fieldDescriptor;
    private final PBValueReader elementReader;
    private final ArrayData.ElementGetter elementGetter;

    public PBRepeatedFieldSetter(
            Descriptors.FieldDescriptor fieldDescriptor,
            LogicalType logicalType,
            PBFactory pbFactory) {
        this.fieldDescriptor = fieldDescriptor;
        final LogicalType elementType = logicalType.getChildren().get(0);
        this.elementReader = pbFactory.createValueReader(fieldDescriptor, elementType);
        this.elementGetter = ArrayData.createElementGetter(elementType);
    }

    @Override
    public void set(CodedInputStream stream, GenericRowData rowData, int rowIndex)
            throws IOException {
        final GenericArrayData2 values = currentValues(rowData, rowIndex);
        final int wireType = WireFormat.getTagWireType(stream.getLastTag());
        if (fieldDescriptor.isPackable() && wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
            final int length = stream.readRawVarint32();
            final int oldLimit = stream.pushLimit(length);
            while (!stream.isAtEnd()) {
                values.add(elementReader.read(stream));
            }
            stream.popLimit(oldLimit);
        } else {
            values.add(elementReader.read(stream));
        }
        rowData.setField(rowIndex, values);
    }

    private GenericArrayData2 currentValues(GenericRowData rowData, int rowIndex) {
        if (rowData.isNullAt(rowIndex)) {
            return new GenericArrayData2();
        }
        final ArrayData current = rowData.getArray(rowIndex);
        if (current instanceof GenericArrayData2 genericArrayData2) {
            return genericArrayData2;
        }
        final GenericArrayData2 values = new GenericArrayData2();
        for (int i = 0; i < current.size(); i++) {
            values.add(elementGetter.getElementOrNull(current, i));
        }
        return values;
    }
}
