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
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

/** EnumPBValueReader. */
public class EnumPBValueReader extends AbstractTypedPBValueReader {

    private final Map<Integer, StringData> valueMapping;

    public EnumPBValueReader(LogicalType logicalType, Descriptors.FieldDescriptor descriptor) {
        super(logicalType, descriptor);
        final Map<Integer, StringData> mapping = new HashMap<>();
        for (var ev : descriptor.getEnumType().getValues()) {
            mapping.put(ev.getNumber(), StringData.fromString(ev.getName()));
        }
        this.valueMapping = mapping;
    }

    @Override
    public Object read(CodedInputStream stream) throws IOException {
        final int enumValue = stream.readEnum();
        if (typeRoot == LogicalTypeRoot.VARCHAR) {
            final var value = valueMapping.get(enumValue);
            return value == null ? BinaryStringData.EMPTY_UTF8 : value;
        }
        if (typeRoot == LogicalTypeRoot.BIGINT) {
            return (long) enumValue;
        }
        return enumValue;
    }
}
