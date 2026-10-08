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

package name.zicat.astatine.format.protobuf.test;

import java.util.Arrays;

import com.google.protobuf.ByteString;
import name.zicat.astatine.format.protobuf.serialize.PbRowDataSerializationSchemaV2;
import name.zicat.astatine.formats.protobuf.Test.NameScoreTs;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Assert;
import org.junit.Test;

/** PbRowDataSerializationSchemaV2Test. */
public class PbRowDataSerializationSchemaV2Test {

    @Test
    public void testSerializeAllFields() throws Exception {
        final var schema = createSchema();
        final var rowData =
                GenericRowData.of(StringData.fromString("zicat"), 100, 12345L);

        final var message = NameScoreTs.parseFrom(schema.serialize(rowData));

        Assert.assertEquals("zicat", message.getName());
        Assert.assertEquals(100, message.getScore());
        Assert.assertEquals(12345L, message.getTs());
    }

    @Test
    public void testSerializeNullFieldsAsProtobufDefaults() throws Exception {
        final var schema = createSchema();
        final RowData rowData = GenericRowData.of(StringData.fromString("zicat"), null, null);

        final var message = NameScoreTs.parseFrom(schema.serialize(rowData));

        Assert.assertEquals("zicat", message.getName());
        Assert.assertEquals(0, message.getScore());
        Assert.assertEquals(0L, message.getTs());
    }

    @Test
    public void testSerializeEmptyAndNonAsciiStringWithoutReplacement() throws Exception {
        final var schema = createSchema();
        final RowData rowData =
                GenericRowData.of(StringData.fromString("你好"), 0, 0L);

        final var serialized = schema.serialize(rowData);
        final var message = NameScoreTs.parseFrom(serialized);

        Assert.assertEquals("你好", message.getName());
        Assert.assertTrue(ByteString.copyFromUtf8("你好").equals(message.getNameBytes()));
    }

    @Test
    public void testSerializeThenDeserialize() throws Exception {
        final var serializationSchema = createSchema();
        final var deserializationSchema =
                new name.zicat.astatine.format.protobuf.ProtobufRowDataDeserializationSchemaV2(
                        false,
                        rowType(),
                        org.apache.flink.api.common.typeinfo.Types.GENERIC(RowData.class),
                        NameScoreTs.class.getName());
        deserializationSchema.open(null);

        final var input = GenericRowData.of(StringData.fromString("zicat"), 100, 12345L);
        final var output = deserializationSchema.deserialize(serializationSchema.serialize(input));

        Assert.assertEquals("zicat", output.getString(0).toString());
        Assert.assertEquals(100, output.getInt(1));
        Assert.assertEquals(12345L, output.getLong(2));
    }

    private static PbRowDataSerializationSchemaV2 createSchema() throws Exception {
        final var config =
                new PbFormatConfig.PbFormatConfigBuilder()
                        .messageClassName(NameScoreTs.class.getName())
                        .build();
        final var schema = new PbRowDataSerializationSchemaV2(rowType(), config);
        schema.open(null);
        return schema;
    }

    private static RowType rowType() {
        return new RowType(
                Arrays.asList(
                        new RowType.RowField("name", new VarCharType()),
                        new RowType.RowField("score", new IntType()),
                        new RowType.RowField("ts", new BigIntType())));
    }
}
