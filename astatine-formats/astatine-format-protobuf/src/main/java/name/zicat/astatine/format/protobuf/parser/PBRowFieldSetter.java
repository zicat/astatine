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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.LogicalType;

/** PBRowFieldSetter. */
public class PBRowFieldSetter implements PBFieldSetter {

    private final PBValueReader valueReader;

    public PBRowFieldSetter(
            Descriptors.FieldDescriptor fieldDescriptor,
            LogicalType logicalType,
            PBFactory pbFactory) {
        this.valueReader = pbFactory.createValueReader(fieldDescriptor, logicalType);
    }

    @Override
    public void set(CodedInputStream stream, GenericRowData rowData, int rowIndex)
            throws IOException {
        rowData.setField(rowIndex, valueReader.read(stream));
    }
}
