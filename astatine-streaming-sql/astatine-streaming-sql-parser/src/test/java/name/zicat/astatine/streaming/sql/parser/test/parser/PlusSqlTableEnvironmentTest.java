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

import name.zicat.astatine.streaming.sql.parser.parser.PlusSqlTableEnvironment;
import name.zicat.astatine.streaming.sql.parser.test.function.NameScore;
import name.zicat.astatine.streaming.sql.parser.test.function.NameSexScoreTransformFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.test.function.NameTsKeySelect;
import name.zicat.astatine.streaming.sql.parser.test.transform.ConnectTransformFactoryTest;
import name.zicat.astatine.streaming.sql.parser.test.transform.TwoTransformFactoryTest;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.stream.Collectors;

import static name.zicat.astatine.streaming.sql.parser.test.transform.TransformFactoryTestBase.*;
import static name.zicat.astatine.streaming.sql.parser.test.transform.TwoTransformFactoryTest.leftSource;
import static name.zicat.astatine.streaming.sql.parser.test.transform.TwoTransformFactoryTest.rightSource;

/** PlusSqlTableEnvironment. */
public class PlusSqlTableEnvironmentTest {

    public static PlusSqlTableEnvironment create(StreamExecutionEnvironment execEnv) {
        execEnv.setParallelism(1);
        final var env = (StreamTableEnvironmentImpl) StreamTableEnvironment.create(execEnv);
        return new PlusSqlTableEnvironment(env);
    }

    @Test
    public void testUnSupportedOperator() {
        final var execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        final var plusTableEnv = create(execEnv);
        final var streamCache = plusTableEnv.streamCache();
        final var tableEnv = plusTableEnv.streamTableEnvironment();
        final var s1 = execEnv.fromElements(Row.of("aaaa", 1), Row.of("bbbb", 2));

        streamCache.put("test_s1", s1);

        final var statement = tableEnv.createStatementSet();
        try {
            plusTableEnv.executeSql(
                    statement,
                    """
                        CREATE STREAM view_result_stream
                        FROM test_s1
                        FLATMAP WITH (
                            'identity' = 'row_2_name_score'
                        )
                        """);
            Assert.fail("unknown operator MAP2, should throw exception");
        } catch (Exception ignore) {

        }
    }

    @Test
    public void testCreateViewFrom() throws Exception {
        final var execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        final var plusTableEnv = create(execEnv);
        final var streamCache = plusTableEnv.streamCache();
        final var tableEnv = plusTableEnv.streamTableEnvironment();
        final var s1 = execEnv.fromElements(Row.of("aaaa", 1), Row.of("bbbb", 2));

        streamCache.put("test_s1", s1);

        final var statement = tableEnv.createStatementSet();
        /*
         stream data
         1. (aaaa, 1) (bbbb, 2)
         2. (aaaa, 1) (bbbb, 2) -- name_score_keyed_process
        */
        plusTableEnv.executeSql(
                statement,
                """
            CREATE VIEW view_result_stream
            FROM test_s1
            MAP WITH (
                'identity' = 'row_2_name_score'
            )
            KEY BY WITH (
                'identity' = 'name_score_key_selector'
            )
            PROCESS WITH (
                'identity' = 'name_score_keyed_process'
            )
            """);

        execAndAssert(
                tableEnv.toDataStream(tableEnv.from("view_result_stream")),
                (AssertHandler<Row>) result -> {
                    Assert.assertEquals(2, result.size());
                    Assert.assertEquals("aaaa", result.get(0).getField(0));
                    Assert.assertEquals("bbbb", result.get(1).getField(0));

                    Assert.assertEquals(1, result.get(0).getField(1));
                    Assert.assertEquals(2, result.get(1).getField(1));
                });
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testConnect() throws Exception {

        final var execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        final var plusTableEnv = create(execEnv);
        final var streamCache = plusTableEnv.streamCache();
        final var ts = System.currentTimeMillis();

        final var leftStream =
                execEnv.addSource(ConnectTransformFactoryTest.leftSource(ts))
                        .keyBy(new NameTsKeySelect<>());
        final var rightStream =
                execEnv.addSource(ConnectTransformFactoryTest.rightSource(ts))
                        .keyBy(new NameTsKeySelect<>());
        streamCache.put("left_stream", leftStream);
        streamCache.put("right_stream", rightStream);
        final var statement = plusTableEnv.streamTableEnvironment().createStatementSet();

        final var sql =
                """
        CREATE STREAM connect_2_stream FROM left_stream
        CONNECT right_stream WITH (
            'identity' = 'name_score_ts_connect'
        )
        """;
        plusTableEnv.executeSql(statement, sql);
        execAndAssert(
                streamCache.get("connect_2_stream"),
                (AssertHandler) result -> ConnectTransformFactoryTest.assertResult(ts, result));
    }

    @Test
    public void testTransformWith() throws Exception {

        final var execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        final var plusTableEnv = create(execEnv);
        final var streamCache = plusTableEnv.streamCache();
        final var ts = System.currentTimeMillis();

        final var leftStream = execEnv.addSource(leftSource(ts));
        final var rightStream = execEnv.addSource(rightSource(ts));

        streamCache.put("left_stream", leftStream);
        streamCache.put("right_stream", rightStream);

        final var statement = plusTableEnv.streamTableEnvironment().createStatementSet();

        final var sql =
                """
            CREATE STREAM join_2_stream FROM left_stream
            TRANSFORM right_stream WITH (
                'identity' = 'name_sex_score_transform'
            )
            """;
        plusTableEnv.executeSql(statement, sql);

        execAndAssert(streamCache.get("join_2_stream"), TwoTransformFactoryTest::assertResult);
        final var sql2 =
                """
            CREATE STREAM join_2_stream_v2 FROM left_stream
            JOIN right_stream WITH (
                'identity' = 'name_sex_score_join'
            )
            """;
        plusTableEnv.executeSql(statement, sql2);

        execAndAssert(streamCache.get("join_2_stream_v2"), TwoTransformFactoryTest::assertResult);

        final var keyedLeftStream =
                leftStream.keyBy(new NameSexScoreTransformFunctionFactory.NameKeySelect<>() {});
        final var keyedRightStream =
                rightStream.keyBy(new NameSexScoreTransformFunctionFactory.NameKeySelect<>() {});

        streamCache.put("keyed_left_stream", keyedLeftStream);
        streamCache.put("keyed_right_stream", keyedRightStream);

        final var sql3 =
                """
            CREATE STREAM join_2_stream_v3 FROM keyed_left_stream
            INTERVAL JOIN keyed_right_stream WITH (
                'identity' = 'name_sex_score_interval_join'
            )
            """;
        plusTableEnv.executeSql(statement, sql3);

        execAndAssert(streamCache.get("join_2_stream_v3"), TwoTransformFactoryTest::assertResult);

        final var sql4 =
                """
            CREATE STREAM join_2_stream_v4 FROM keyed_left_stream
            TRANSFORM keyed_right_stream WITH (
                'identity' = 'name_sex_score_interval_join'
            )
            """;
        plusTableEnv.executeSql(statement, sql4);

        execAndAssert(streamCache.get("join_2_stream_v4"), TwoTransformFactoryTest::assertResult);
    }

    @Test
    public void testIllegalDefine() throws Exception {
        final var execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        final var plusTableEnv = create(execEnv);
        final var streamCache = plusTableEnv.streamCache();

        final var s1 =
                execEnv.fromElements(
                        new Tuple4<>("aaaa", 1, "p1", "sp1"),
                        new Tuple4<>("bbbb", 2, "p2", "sp2"),
                        new Tuple4<>("aaaa", 3, "p3", "sp3"),
                        new Tuple4<>("aaaa", 1, "p4", "sp4"),
                        new Tuple4<>("aaaa", 3, "p5", "sp5"));
        streamCache.put("test_s1", s1);
        final var statement = plusTableEnv.streamTableEnvironment().createStatementSet();

        final var sql =
                """
            CREATE STREAM key_by_stream FROM test_s1
            PROJECT WITH (
                'field_indexes' = '0,1,2'
            )
            MAP WITH (
                'identity' = 'tuple_2_name_score_payload'
            )
            """;
        plusTableEnv.executeSql(statement, sql);
        try {
            plusTableEnv.executeSql(statement, sql);
            Assert.fail("not allow repeat define source");
        } catch (Exception ignore) {
        }

        final var sql2 =
                """
            CREATE STREAM key_by_stream2 FROM test_s1
            PROJECT WITH (
                'field_indexes' = '0,1,3'
            )
            MAP WITH (
                'identity' = 'tuple_2_name_score_payload'
            )
            """;
        plusTableEnv.executeSql(statement, sql2);

        execAndAssert(
                streamCache.get("key_by_stream"),
                result -> {
                    Assert.assertEquals(5, result.size());
                    assertName(result, "aaaa", "bbbb", "aaaa", "aaaa", "aaaa");
                    assertScore(result, 1, 2, 3, 1, 3);
                    assertPayload(result, "p1", "p2", "p3", "p4", "p5");
                });

        execAndAssert(
                streamCache.get("key_by_stream2"),
                result -> {
                    Assert.assertEquals(5, result.size());
                    assertName(result, "aaaa", "bbbb", "aaaa", "aaaa", "aaaa");
                    assertScore(result, 1, 2, 3, 1, 3);
                    assertPayload(result, "sp1", "sp2", "sp3", "sp4", "sp5");
                });
    }

    @Test
    public void testReducePlusSql() throws Exception {

        final var execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        final var plusTableEnv = create(execEnv);
        final var streamCache = plusTableEnv.streamCache();

        final var s1 =
                execEnv.fromElements(
                        new NameScore("n1", 1), new NameScore("n2", 2), new NameScore("n1", 2));
        streamCache.put("test_s1", s1);
        final var statement = plusTableEnv.streamTableEnvironment().createStatementSet();
        plusTableEnv.executeSql(
                statement,
                """
            CREATE STREAM reduce_stream FROM test_s1
            KEY BY WITH (
                'identity' = 'name_score_key_selector'
            )
            REDUCE WITH (
                'identity' = 'name_score_reduce'
            )""");

        execAndAssert(
                streamCache.get("reduce_stream"),
                result -> {
                    Assert.assertEquals(3, result.size());

                    assertName(result, "n1", "n2", "n1");
                    assertScore(result, 1, 2, 3);
                });
    }

    @Test
    public void testAggregationPlusSql() throws Exception {

        final var execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        final var plusTableEnv = create(execEnv);
        final var streamCache = plusTableEnv.streamCache();

        final var s1 =
                execEnv.fromElements(
                        new Tuple4<>("aaaa", 1, "p1", 4L),
                        new Tuple4<>("bbbb", 2, "p2", 5L),
                        new Tuple4<>("aaaa", 3, "p3", 6L),
                        new Tuple4<>("aaaa", 1, "p4", 7L),
                        new Tuple4<>("aaaa", 3, "p5", 8L));
        streamCache.put("test_s1", s1);
        final var statement = plusTableEnv.streamTableEnvironment().createStatementSet();
        plusTableEnv.executeSql(
                statement,
                """
            CREATE STREAM key_by_stream FROM test_s1
            PROJECT WITH (
                'field_indexes' = '0,1,2'
            )
            TRANSFORM WITH (
                'identity' = 'tuple_2_name_score_payload'
            )
            KEY BY WITH (
                'identity' = 'name_score_payload_key_selector'
            )
            """);

        testSum(plusTableEnv, statement);
        testMin(plusTableEnv, statement);
        testMax(plusTableEnv, statement);
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void testRegisterPlusSql() throws Exception {

        final var execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        final var plusTableEnv = create(execEnv);
        final var streamCache = plusTableEnv.streamCache();

        final var s1 = execEnv.fromElements(Row.of("aaaa", 1), Row.of("bbbb", 2));
        final var s2 =
                execEnv.fromElements(Row.of("cccc", 3), Row.of("aaab", 4), Row.of("aaaa", 5));

        streamCache.put("test_s1", s1);
        streamCache.put("test_s2", s2);

        final var statement = plusTableEnv.streamTableEnvironment().createStatementSet();
        /*
                stream data
                1. (aaaa, 1) (bbbb, 2) (cccc, 3) (aaab, 4) (aaaa, 5)
                2. (aaaa, 1) (bbbb, 2) (cccc, 3) (aaab, 4) (aaaa, 6) -- name_score_keyed_process
                3. (aaaa, 1) (aaaa, 1) (bbbb, 2) (bbbb, 2) (cccc, 3) (cccc, 3) (aaab, 4) (aaab, 4)
        (aaaa, 6) (aaaa, 6) -- name_score_flat_map
                4. (aaaa, 1) (aaaa, 1) (aaab, 4) (aaab, 4) (aaaa, 6) (aaaa, 6) -- name_score_filter
               */
        plusTableEnv.executeSql(
                statement,
                """
            CREATE STREAM result_stream
            FROM test_s1
            UNION test_s2
            MAP WITH (
                'identity' = 'row_2_name_score'
            )
            KEY BY WITH (
                'identity' = 'name_score_key_selector'
            )
            PROCESS WITH (
                'identity' = 'name_score_keyed_process'
            )
            FLAT MAP WITH (
                'identity' = 'name_score_flat_map'
            )
            FILTER WITH (
                'identity' = 'name_score_filter',
                'name_prefix' = 'aaa'
            )
            """);
        execAndAssert(
                streamCache.get("result_stream"),
                result -> {
                    Assert.assertEquals(6, result.size());
                    final var groupByName =
                            result.stream()
                                    .map(NameScore.class::cast)
                                    .collect(
                                            Collectors.groupingBy(
                                                    NameScore::getName,
                                                    Collectors.maxBy(
                                                            Comparator.comparingInt(
                                                                    NameScore::getScore))));
                    Assert.assertEquals(6, groupByName.get("aaaa").get().getScore());
                    Assert.assertEquals(4, groupByName.get("aaab").get().getScore());
                });
    }

    private static void testSum(PlusSqlTableEnvironment plusTableEnv, StreamStatementSet statement)
            throws Exception {
        plusTableEnv.executeSql(
                statement,
                """
            CREATE STREAM sum_result_stream FROM key_by_stream
            SUM WITH (
                'field' = 'score'
            )
            """);
        execAndAssert(
                plusTableEnv.streamCache().get("sum_result_stream"),
                result -> {
                    Assert.assertEquals(5, result.size());
                    assertName(result, "aaaa", "bbbb", "aaaa", "aaaa", "aaaa");
                    assertScore(result, 1, 2, 4, 5, 8);
                    assertPayload(result, "p1", "p2", "p1", "p1", "p1");
                });

        plusTableEnv.executeSql(
                statement,
                """
            CREATE STREAM sum_result_stream2 FROM key_by_stream
            TRANSFORM WITH (
                'identity' = 'aggregation_sum',
                'field' = 'score'
            )
            """);
        execAndAssert(
                plusTableEnv.streamCache().get("sum_result_stream2"),
                result -> {
                    Assert.assertEquals(5, result.size());
                    assertName(result, "aaaa", "bbbb", "aaaa", "aaaa", "aaaa");
                    assertScore(result, 1, 2, 4, 5, 8);
                    assertPayload(result, "p1", "p2", "p1", "p1", "p1");
                });
    }

    private static void testMin(PlusSqlTableEnvironment plusTableEnv, StreamStatementSet statement)
            throws Exception {
        final var streamCache = plusTableEnv.streamCache();
        plusTableEnv.executeSql(
                statement,
                """
            CREATE STREAM min_result_stream FROM key_by_stream
            MIN WITH (
                'field' = 'score'
            )
            """);
        execAndAssert(
                streamCache.get("min_result_stream"),
                result -> {
                    Assert.assertEquals(5, result.size());
                    assertName(result, "aaaa", "bbbb", "aaaa", "aaaa", "aaaa");
                    assertScore(result, 1, 2, 1, 1, 1);
                    assertPayload(result, "p1", "p2", "p1", "p1", "p1");
                });

        plusTableEnv.executeSql(
                statement,
                """
            CREATE STREAM min_by_result_stream FROM key_by_stream
            MIN BY WITH (
                'field' = 'score'
            )
            """);
        execAndAssert(
                streamCache.get("min_by_result_stream"),
                result -> {
                    Assert.assertEquals(5, result.size());
                    assertName(result, "aaaa", "bbbb", "aaaa", "aaaa", "aaaa");
                    assertScore(result, 1, 2, 1, 1, 1);
                    assertPayload(result, "p1", "p2", "p1", "p1", "p1");
                });

        plusTableEnv.executeSql(
                statement,
                """
            CREATE STREAM min_by_2_result_stream FROM key_by_stream
            MIN BY WITH (
                'field' = 'score',
                'first' = 'false'
            )
            """);
        execAndAssert(
                streamCache.get("min_by_2_result_stream"),
                result -> {
                    Assert.assertEquals(5, result.size());
                    assertName(result, "aaaa", "bbbb", "aaaa", "aaaa", "aaaa");
                    assertScore(result, 1, 2, 1, 1, 1);
                    assertPayload(result, "p1", "p2", "p1", "p4", "p4");
                });
    }

    private static void testMax(PlusSqlTableEnvironment plusTableEnv, StreamStatementSet statement)
            throws Exception {
        final var streamCache = plusTableEnv.streamCache();
        plusTableEnv.executeSql(
                statement,
                """
            CREATE STREAM max_result_stream FROM key_by_stream
            MAX WITH (
                'field' = 'score'
            )
            """);
        execAndAssert(
                streamCache.get("max_result_stream"),
                result -> {
                    Assert.assertEquals(5, result.size());
                    assertName(result, "aaaa", "bbbb", "aaaa", "aaaa", "aaaa");
                    assertScore(result, 1, 2, 3, 3, 3);
                    assertPayload(result, "p1", "p2", "p1", "p1", "p1");
                });

        plusTableEnv.executeSql(
                statement,
                """
            CREATE STREAM max_by_result_stream FROM key_by_stream
            MAX BY WITH (
                'field' = 'score'
            )
            """);
        execAndAssert(
                streamCache.get("max_by_result_stream"),
                result -> {
                    Assert.assertEquals(5, result.size());
                    assertName(result, "aaaa", "bbbb", "aaaa", "aaaa", "aaaa");
                    assertScore(result, 1, 2, 3, 3, 3);
                    assertPayload(result, "p1", "p2", "p3", "p3", "p3");
                });

        plusTableEnv.executeSql(
                statement,
                """
            CREATE STREAM max_by_2_result_stream FROM key_by_stream
            MAX BY WITH (
                'field' = 'score',
                'first' = 'false'
            )
            """);
        execAndAssert(
                streamCache.get("max_by_2_result_stream"),
                result -> {
                    Assert.assertEquals(5, result.size());
                    assertName(result, "aaaa", "bbbb", "aaaa", "aaaa", "aaaa");
                    assertScore(result, 1, 2, 3, 3, 3);
                    assertPayload(result, "p1", "p2", "p3", "p3", "p5");
                });
    }
}
