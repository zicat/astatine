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

import static name.zicat.astatine.format.protobuf.PBUtils.defaultValue;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;
import java.io.IOException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/** PBMessageValueReader. */
public class PBMessageValueReader extends AbstractTypedPBValueReader {

    protected final PBFieldDispatcher fieldDispatcher;
    protected final int fieldCount;
    protected final Object[] defaultValues;

    public PBMessageValueReader(
            LogicalType logicalType, Descriptors.Descriptor descriptor, PBFactory pbFactory) {
        super(logicalType, descriptor);
        final RowType rowType = (RowType) logicalType;
        this.fieldCount = rowType.getFieldCount();
        this.fieldDispatcher = pbFactory.createFieldDispatcher(descriptor.getFields(), rowType);
        this.defaultValues = defaultValue(rowType);
    }

    @Override
    public Object read(CodedInputStream stream) throws IOException {
        final int length = stream.readRawVarint32();
        final int oldLimit = stream.pushLimit(length);
        final var row = new GenericRowData(fieldCount);
        applyDispatcher(fieldDispatcher, stream, row);
        stream.popLimit(oldLimit);
        for (int i = 0; i < fieldCount; i++) {
            if (row.isNullAt(i)) {
                row.setField(i, defaultValues[i]);
            }
        }
        return row;
    }

    /**
     * apply dispatcher.
     *
     * @param dispatcher dispatcher
     * @param stream stream
     * @param rowData rowData
     * @throws IOException IOException
     */
    public static void applyDispatcher(
            PBFieldDispatcher dispatcher, CodedInputStream stream, GenericRowData rowData)
            throws IOException {
        while (!stream.isAtEnd()) {
            final int tag = stream.readTag();
            final int fieldNumber = com.google.protobuf.WireFormat.getTagFieldNumber(tag);
            final var binding = dispatcher.get(fieldNumber);
            if (binding == null) {
                stream.skipField(tag);
                continue;
            }
            binding.fieldSetter().set(stream, rowData, binding.rowIndex());
        }
    }
}
