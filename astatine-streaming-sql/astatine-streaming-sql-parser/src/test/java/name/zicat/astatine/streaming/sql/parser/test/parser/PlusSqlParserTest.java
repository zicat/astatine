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

package name.zicat.astatine.streaming.sql.parser.test.parser;

import name.zicat.astatine.streaming.sql.parser.parser.PlusSqlParser;
import name.zicat.astatine.streaming.sql.parser.parser.StreamOperation;
import name.zicat.astatine.streaming.sql.parser.parser.StreamType;

import org.junit.Assert;
import org.junit.Test;

/** PlusSqlExtensionsListenerTest. */
public class PlusSqlParserTest {

    @Test
    public void testInsertIntoParse() {
        final var sqlText =
                """
            INSERT INTO S1
            FROM t1
            MAP WITH (
                'identity' = 'row_2_name_score'
            )
            filter with (
                'identity' = 'name_score_filter',
                'prefix' = 'bb'
            )
            KEY by WITH (
                'identity' = 'name_score_key_select'
            )
            PROCESS WITH (
                'identity' = 'name_score_key_process'
            )
            FLAT MAP WITH (
                'identity' = 'name_score_key_flat_map'
            )
            UNION S8 union s9
        """;
        final var parser = new PlusSqlParser();
        final var streamOperation = parser.parse(sqlText);
        Assert.assertEquals(StreamType.INSERT_INTO, streamOperation.streamType());

        final var sqlText2 = """
            insert into S2 from t2
        """;
        final var streamOperation2 = parser.parse(sqlText2);
        Assert.assertEquals(StreamType.INSERT_INTO, streamOperation2.streamType());

        assertStreamOptions(streamOperation, streamOperation2);
    }

    @Test
    public void testCreateStreamParse() {
        final var sqlText =
                """
            CREATE STREAM S1
            FROM t1
            MAP WITH (
                'identity' = 'row_2_name_score'
            )
            filter with (
                'identity' = 'name_score_filter',
                'prefix' = 'bb'
            )
            KEY by WITH (
                'identity' = 'name_score_key_select'
            )
            PROCESS WITH (
                'identity' = 'name_score_key_process'
            )
            FLAT MAP WITH (
                'identity' = 'name_score_key_flat_map'
            )
            UNION S8 union s9
        """;
        final var parser = new PlusSqlParser();
        final var streamOperation = parser.parse(sqlText);
        Assert.assertEquals(StreamType.CREATE_STREAM, streamOperation.streamType());

        final var sqlText2 = """
            create stream S2 from t2
        """;
        final var streamOperation2 = parser.parse(sqlText2);

        assertStreamOptions(streamOperation, streamOperation2);
        Assert.assertEquals(StreamType.CREATE_STREAM, streamOperation2.streamType());
    }

    /**
     * assert.
     *
     * @param streamOperation streamOperation
     * @param streamOperation2 streamOperation2
     */
    private static void assertStreamOptions(
            StreamOperation streamOperation, StreamOperation streamOperation2) {
        Assert.assertEquals("S1", streamOperation.name());
        Assert.assertEquals("t1", streamOperation.source());

        Assert.assertEquals(7, streamOperation.streamOperatorOperations().size());

        Assert.assertEquals("MAP", streamOperation.streamOperatorOperations().get(0).type());
        Assert.assertEquals("FILTER", streamOperation.streamOperatorOperations().get(1).type());
        Assert.assertEquals("KEY BY", streamOperation.streamOperatorOperations().get(2).type());
        Assert.assertEquals("PROCESS", streamOperation.streamOperatorOperations().get(3).type());
        Assert.assertEquals("FLAT MAP", streamOperation.streamOperatorOperations().get(4).type());
        Assert.assertEquals("UNION", streamOperation.streamOperatorOperations().get(5).type());
        Assert.assertEquals("UNION", streamOperation.streamOperatorOperations().get(6).type());

        Assert.assertEquals(
                1, streamOperation.streamOperatorOperations().get(0).properties().size());
        Assert.assertEquals(
                2, streamOperation.streamOperatorOperations().get(1).properties().size());
        Assert.assertEquals(
                1, streamOperation.streamOperatorOperations().get(2).properties().size());
        Assert.assertEquals(
                1, streamOperation.streamOperatorOperations().get(3).properties().size());
        Assert.assertEquals(
                1, streamOperation.streamOperatorOperations().get(4).properties().size());
        Assert.assertEquals(
                0, streamOperation.streamOperatorOperations().get(5).properties().size());
        Assert.assertEquals(
                0, streamOperation.streamOperatorOperations().get(6).properties().size());

        Assert.assertEquals(
                "row_2_name_score",
                streamOperation.streamOperatorOperations().get(0).properties().get("identity"));
        Assert.assertEquals(
                "name_score_filter",
                streamOperation.streamOperatorOperations().get(1).properties().get("identity"));
        Assert.assertEquals(
                "bb", streamOperation.streamOperatorOperations().get(1).properties().get("prefix"));
        Assert.assertEquals(
                "name_score_key_select",
                streamOperation.streamOperatorOperations().get(2).properties().get("identity"));
        Assert.assertEquals(
                "name_score_key_process",
                streamOperation.streamOperatorOperations().get(3).properties().get("identity"));
        Assert.assertEquals(
                "name_score_key_flat_map",
                streamOperation.streamOperatorOperations().get(4).properties().get("identity"));

        Assert.assertEquals("S8", streamOperation.streamOperatorOperations().get(5).source());
        Assert.assertEquals("s9", streamOperation.streamOperatorOperations().get(6).source());

        Assert.assertEquals("S2", streamOperation2.name());
        Assert.assertEquals("t2", streamOperation2.source());
        Assert.assertTrue(streamOperation2.streamOperatorOperations().isEmpty());
    }
}
