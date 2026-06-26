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

import name.zicat.astatine.format.protobuf.PBUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;

/** PBMapValueReader. */
public class PBMapValueReader extends AbstractTypedPBValueReader {

    private static final String NAME_PB_MAP_KEY = "key";
    private static final String NAME_PB_MAP_VALUE = "value";

    private final Descriptors.FieldDescriptor keyFieldDescriptor;
    private final Descriptors.FieldDescriptor valueFieldDescriptor;
    private final PBValueReader keyReader;
    private final PBValueReader valueReader;
    private final Object defaultKey;
    private final Object defaultValue;
    private final LogicalType keyType;
    private final LogicalType valueType;

    public PBMapValueReader(
            Descriptors.FieldDescriptor fieldDescriptor,
            LogicalType logicalType,
            PBFactory pbFactory) {
        super(logicalType, fieldDescriptor);
        final var mapType = (MapType) logicalType;
        this.keyType = mapType.getKeyType();
        this.valueType = mapType.getValueType();
        final var entryDescriptor = fieldDescriptor.getMessageType();
        this.keyFieldDescriptor = entryDescriptor.findFieldByName(NAME_PB_MAP_KEY);
        this.valueFieldDescriptor = entryDescriptor.findFieldByName(NAME_PB_MAP_VALUE);
        this.keyReader = pbFactory.createValueReader(keyFieldDescriptor, keyType);
        this.valueReader = pbFactory.createValueReader(valueFieldDescriptor, valueType);
        this.defaultKey = PBUtils.defaultValue(keyType);
        this.defaultValue = PBUtils.defaultValue(valueType);
    }

    @Override
    public Object read(CodedInputStream stream) throws IOException {
        final int length = stream.readRawVarint32();
        final int oldLimit = stream.pushLimit(length);
        Object key = null;
        Object value = null;
        while (!stream.isAtEnd()) {
            final int tag = stream.readTag();
            final int fieldNumber = WireFormat.getTagFieldNumber(tag);
            if (fieldNumber == keyFieldDescriptor.getNumber()) {
                key = keyReader.read(stream);
            } else if (fieldNumber == valueFieldDescriptor.getNumber()) {
                value = valueReader.read(stream);
            } else {
                stream.skipField(tag);
            }
        }
        stream.popLimit(oldLimit);
        final var map = new GenericMapData2();
        map.put(key == null ? defaultKey : key, value == null ? defaultValue : value);
        return map;
    }

    public LogicalType keyType() {
        return keyType;
    }

    public LogicalType valueType() {
        return valueType;
    }
}
