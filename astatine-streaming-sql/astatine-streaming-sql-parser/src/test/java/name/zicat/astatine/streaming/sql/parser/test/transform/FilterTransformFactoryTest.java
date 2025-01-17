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

import name.zicat.astatine.streaming.sql.parser.function.FunctionFactory;
import name.zicat.astatine.streaming.sql.parser.test.function.NameScore;
import name.zicat.astatine.streaming.sql.parser.test.function.NameScoreFilterFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.transform.FilterTransformFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformFactory;

import org.apache.flink.configuration.Configuration;
import org.junit.Assert;
import org.junit.Test;

/** FilterTransformFactoryTest. */
public class FilterTransformFactoryTest extends TransformFactoryTestBase {

    @Test
    public void test() throws Exception {

        final var configuration = new Configuration();
        configuration.set(FunctionFactory.OPTION_FUNCTION_IDENTITY, "name_score_filter");
        configuration.set(NameScoreFilterFunctionFactory.OPTION_NAME_PREFIX, "aaaa");

        final var context = createContext(configuration);

        final var testStream = env.fromElements(new NameScore("aaaa", 1), new NameScore("aaab", 2));
        final var factory =
                TransformFactory.findFactory(FilterTransformFactory.IDENTITY)
                        .cast(FilterTransformFactory.class);

        execAndAssert(
                factory.transform(context, testStream),
                result -> {
                    Assert.assertEquals(1, result.size());
                    assertName(result, "aaaa");
                    assertScore(result, 1);
                });

        try {
            execAndAssert(
                    factory.transform(context, testStream, testStream),  result -> {});
            Assert.fail("SingleOperatorFactory only support one stream");
        } catch (Exception ignore) {

        }
    }
}
