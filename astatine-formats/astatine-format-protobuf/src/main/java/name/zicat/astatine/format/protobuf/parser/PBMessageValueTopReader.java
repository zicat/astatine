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

/** PBMessageTopValueReader. */
public class PBMessageValueTopReader extends PBMessageValueReader {

    public PBMessageValueTopReader(
            LogicalType logicalType, Descriptors.Descriptor descriptor, PBFactory pbFactory) {
        super(logicalType, descriptor, pbFactory);
    }

    @Override
    public Object read(CodedInputStream stream) throws IOException {
        final var row = new GenericRowData(fieldCount);
        applyDispatcher(fieldDispatcher, stream, row);
        for (int i = 0; i < fieldCount; i++) {
            if (row.isNullAt(i)) {
                row.setField(i, defaultValues[i]);
            }
        }
        return row;
    }
}
