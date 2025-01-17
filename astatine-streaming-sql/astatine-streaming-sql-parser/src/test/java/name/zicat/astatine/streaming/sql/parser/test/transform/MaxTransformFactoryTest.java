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
import name.zicat.astatine.streaming.sql.parser.transform.MaxTransformFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformFactory;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.junit.Assert;
import org.junit.Test;

/** MaxTransformFactoryTest. */
public class MaxTransformFactoryTest extends TransformFactoryTestBase {

    @SuppressWarnings("unchecked")
    @Test
    public void test() throws Exception {

        final var configuration = new Configuration();
        configuration.set(AggregationFunctions.OPTION_AGGREGATION_FIELD, "score");
        final var context = createContext(configuration);

        final var testStream =
                env.fromElements(
                        new NameScorePayload("aaaa", 1, "p1"),
                        new NameScorePayload("aaab", 2, "p2"),
                        new NameScorePayload("aaaa", 0, "p3"));
        final var factory =
                TransformFactory.findFactory(MaxTransformFactory.IDENTITY)
                        .cast(MaxTransformFactory.class);
        execAndAssert(
                factory.transform(
                        context,
                        testStream.keyBy(
                                (KeySelector<NameScorePayload, String>) NameScore::getName)),
                result -> {
                    Assert.assertEquals(3, result.size());
                    assertName(result, "aaaa", "aaab", "aaaa");
                    assertScore(result, 1, 2, 1);
                    assertPayload(result, "p1", "p2", "p1");
                });

        final var configuration2 = new Configuration();
        configuration2.set(AggregationFunctions.OPTION_AGGREGATION_POSITION, 1);

        execAndAssert(
                factory.transform(
                                context.withConfig(configuration2),
                                testStream
                                        .map(new SumTransformFactoryTest.NameScore2Tuple())
                                        .keyBy(new SumTransformFactoryTest.KeySelectTuple()))
                        .map(new SumTransformFactoryTest.Tuple2NameScore())
                        .returns(NameScorePayload.class),
                result -> {
                    Assert.assertEquals(3, result.size());
                    assertName(result, "aaaa", "aaab", "aaaa");
                    assertScore(result, 1, 2, 1);
                    assertPayload(result, "p1", "p2", "p1");
                });
    }
}
