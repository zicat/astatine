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

import java.io.IOException;
import java.util.Arrays;

import name.zicat.astatine.format.protobuf.ProtobufRowDataDeserializationSchemaV2;
import name.zicat.astatine.formats.protobuf.Test.NameScoreTs;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Assert;
import org.junit.Test;

/** ProtobufRowDataDeserializationSchemaV2Test. */
public class ProtobufRowDataDeserializationSchemaV2Test {

    @Test
    public void testDeserialize() throws Exception {
        final var schema = createSchema(false);
        final var message =
                NameScoreTs.newBuilder().setName("zicat").setScore(100).setTs(12345L).build();

        final var rowData = schema.deserialize(message.toByteArray());

        Assert.assertEquals(3, rowData.getArity());
        Assert.assertEquals("zicat", rowData.getString(0).toString());
        Assert.assertEquals(100, rowData.getInt(1));
        Assert.assertEquals(12345L, rowData.getLong(2));
        Assert.assertFalse(schema.isEndOfStream(rowData));
        Assert.assertEquals(Types.GENERIC(RowData.class), schema.getProducedType());
    }

    @Test
    public void testDeserializeDefaultValues() throws Exception {
        final var schema = createSchema(false);
        final var message = NameScoreTs.newBuilder().setName("zicat").build();

        final var rowData = schema.deserialize(message.toByteArray());

        Assert.assertEquals("zicat", rowData.getString(0).toString());
        Assert.assertEquals(0, rowData.getInt(1));
        Assert.assertEquals(0L, rowData.getLong(2));
    }

    @Test
    public void testDeserializeIgnoreParseErrors() throws Exception {
        final var schema = createSchema(true);

        Assert.assertNull(schema.deserialize(new byte[] {10, 100}));
    }

    @Test(expected = IOException.class)
    public void testDeserializeThrowExceptionWhenParseError() throws Exception {
        final var schema = createSchema(false);

        schema.deserialize(new byte[] {10, 100});
    }

    private static ProtobufRowDataDeserializationSchemaV2 createSchema(boolean ignoreParseErrors)
            throws Exception {
        final var rowType = rowType();
        final var schema =
                new ProtobufRowDataDeserializationSchemaV2(
                        ignoreParseErrors,
                        rowType,
                        Types.GENERIC(RowData.class),
                        NameScoreTs.class.getName());
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
