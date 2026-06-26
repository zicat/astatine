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
import java.io.IOException;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.types.logical.LogicalType;

/** PBMapFieldSetter. */
public class PBMapFieldSetter implements PBFieldSetter {

    private final PBMapValueReader reader;
    private final ArrayData.ElementGetter keyGetter;
    private final ArrayData.ElementGetter valueGetter;

    public PBMapFieldSetter(
            Descriptors.FieldDescriptor fieldDescriptor,
            LogicalType logicalType,
            PBFactory pbFactory) {
        this.reader = (PBMapValueReader) pbFactory.createValueReader(fieldDescriptor, logicalType);
        this.keyGetter = ArrayData.createElementGetter(reader.keyType());
        this.valueGetter = ArrayData.createElementGetter(reader.valueType());
    }

    @Override
    public void set(CodedInputStream stream, GenericRowData rowData, int rowIndex)
            throws IOException {
        final GenericMapData2 entry = (GenericMapData2) reader.read(stream);
        final GenericMapData2 current = currentMap(rowData, rowIndex);
        current.putAll(entry.getMap());
        rowData.setField(rowIndex, current);
    }

    private GenericMapData2 currentMap(GenericRowData rowData, int rowIndex) {
        if (rowData.isNullAt(rowIndex)) {
            return new GenericMapData2();
        }
        final MapData current = rowData.getMap(rowIndex);
        if (current instanceof GenericMapData2 genericMapData2) {
            return genericMapData2;
        }
        final GenericMapData2 copied = new GenericMapData2();
        final var keyArray = current.keyArray();
        final var valueArray = current.valueArray();
        for (int i = 0; i < current.size(); i++) {
            copied.put(
                    keyGetter.getElementOrNull(keyArray, i),
                    valueGetter.getElementOrNull(valueArray, i));
        }
        return copied;
    }
}
