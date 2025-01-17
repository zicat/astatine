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

package name.zicat.astatine.streaming.sql.parser.test.transform;

import name.zicat.astatine.streaming.sql.parser.function.aggregation.AggregationFunctions;
import name.zicat.astatine.streaming.sql.parser.test.function.NameScore;
import name.zicat.astatine.streaming.sql.parser.test.function.NameScorePayload;
import name.zicat.astatine.streaming.sql.parser.transform.SumTransformFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformFactory;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.junit.Assert;
import org.junit.Test;

/** SumTransformFactoryTest. */
public class SumTransformFactoryTest extends TransformFactoryTestBase {
    @SuppressWarnings("unchecked")
    @Test
    public void test() throws Exception {

        final var context = createContext();

        final var testStream =
                env.fromElements(
                        new NameScorePayload("aaaa", 1, "p1"),
                        new NameScorePayload("aaab", 2, "p2"),
                        new NameScorePayload("aaaa", 3, "p3"));
        final var factory =
                TransformFactory.findFactory(SumTransformFactory.IDENTITY)
                        .cast(SumTransformFactory.class);
        try {
            // not key by
            factory.transform(context, testStream);
            Assert.fail();
        } catch (Exception ignore) {
        }

        final var keyStream =
                testStream.keyBy((KeySelector<NameScorePayload, String>) NameScore::getName);
        try {
            // not set field or position params
            factory.transform(context, keyStream);
            Assert.fail();
        } catch (Exception ignore) {

        }

        final var configuration = new Configuration();
        configuration.set(AggregationFunctions.OPTION_AGGREGATION_FIELD, "score");
        execAndAssert(
                factory.transform(context.withConfig(configuration), keyStream),
                result -> {
                    Assert.assertEquals(3, result.size());
                    assertName(result, "aaaa", "aaab", "aaaa");
                    assertScore(result, 1, 2, 4);
                    assertPayload(result, "p1", "p2", "p1");
                });

        final var configuration2 = new Configuration();
        configuration2.set(AggregationFunctions.OPTION_AGGREGATION_POSITION, 1);
        // position require class is case class or tuple
        execAndAssert(
                factory.transform(
                                context.withConfig(configuration2),
                                testStream.map(new NameScore2Tuple()).keyBy(new KeySelectTuple()))
                        .map(new Tuple2NameScore())
                        .returns(NameScorePayload.class),
                result -> {
                    Assert.assertEquals(3, result.size());
                    assertName(result, "aaaa", "aaab", "aaaa");
                    assertScore(result, 1, 2, 4);
                    assertPayload(result, "p1", "p2", "p1");
                });
    }

    @SuppressWarnings("rawtypes")
    public static class Tuple2NameScore implements MapFunction {

        @SuppressWarnings("unchecked")
        @Override
        public Object map(Object o) {
            Tuple3<String, Integer, String> t3 = (Tuple3<String, Integer, String>) o;
            return new NameScorePayload(t3.f0, t3.f1, t3.f2);
        }
    }

    public static class KeySelectTuple
            implements KeySelector<Tuple3<String, Integer, String>, String> {

        @Override
        public String getKey(Tuple3<String, Integer, String> t3) {
            return t3.f0;
        }
    }

    public static class NameScore2Tuple
            implements MapFunction<NameScorePayload, Tuple3<String, Integer, String>> {

        @Override
        public Tuple3<String, Integer, String> map(NameScorePayload nameScore) {
            return new Tuple3<>(nameScore.getName(), nameScore.getScore(), nameScore.getPayload());
        }
    }
}
