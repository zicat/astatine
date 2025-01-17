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

import name.zicat.astatine.streaming.sql.parser.test.function.NameScore;
import name.zicat.astatine.streaming.sql.parser.transform.FlatMapTransformFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformFactory;

import org.junit.Assert;
import org.junit.Test;

/** FlatMapTransformFactoryTest. */
public class FlatMapTransformFactoryTest extends TransformFactoryTestBase {

    @Test
    public void test() throws Exception {
        final var context = createContext("name_score_flat_map");
        final var testStream = env.fromElements(new NameScore("aaaa", 1), new NameScore("aaab", 2));
        final var factory =
                TransformFactory.findFactory(FlatMapTransformFactory.IDENTITY)
                        .cast(FlatMapTransformFactory.class);
        execAndAssert(
                factory.transform(context, testStream),
                result -> {
                    Assert.assertEquals(4, result.size());
                    assertName(result, "aaaa", "aaaa", "aaab", "aaab");
                    assertScore(result, 1, 1, 2, 2);
                });
    }
}
