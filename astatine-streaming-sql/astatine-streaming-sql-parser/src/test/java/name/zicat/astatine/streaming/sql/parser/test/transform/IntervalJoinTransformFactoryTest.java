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

import name.zicat.astatine.streaming.sql.parser.test.function.NameSexScoreTransformFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.transform.IntervalJoinTransformFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformFactory;

import org.junit.Test;

/** IntervalJoinTransformFactoryTest. */
public class IntervalJoinTransformFactoryTest extends TransformFactoryTestBase {

    @Test
    public void test() throws Exception {
        env.getConfig().setAutoWatermarkInterval(200);
        final var context = createContext("name_sex_score_interval_join");
        long ts = System.currentTimeMillis();
        final var leftStream =
                env.addSource(TwoTransformFactoryTest.leftSource(ts))
                        .keyBy(new NameSexScoreTransformFunctionFactory.NameKeySelect<>() {});
        final var rightStream =
                env.addSource(TwoTransformFactoryTest.rightSource(ts))
                        .keyBy(new NameSexScoreTransformFunctionFactory.NameKeySelect<>() {});

        final var factory =
                TransformFactory.findFactory(IntervalJoinTransformFactory.IDENTITY)
                        .cast(IntervalJoinTransformFactory.class);
        execAndAssert(
                factory.transform(context, leftStream, rightStream),
                TwoTransformFactoryTest::assertResult);
    }
}
