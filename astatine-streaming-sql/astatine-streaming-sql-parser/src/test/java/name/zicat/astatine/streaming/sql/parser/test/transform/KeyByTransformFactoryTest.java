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
import name.zicat.astatine.streaming.sql.parser.transform.KeyByTransformFactory;
import name.zicat.astatine.streaming.sql.parser.transform.ProcessTransformFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformFactory;

import org.junit.Test;

/** KeyByTransformFactoryTest. */
public class KeyByTransformFactoryTest extends TransformFactoryTestBase {

    @Test
    public void test() throws Exception {
        final var context = createContext("name_score_key_selector");
        final var testStream =
                env.fromElements(
                        new NameScore("aaaa", 1),
                        new NameScore("aaab", 2),
                        new NameScore("aaaa", 3),
                        new NameScore("aaab", 4));
        final var factory =
                TransformFactory.findFactory(KeyByTransformFactory.IDENTITY)
                        .cast(KeyByTransformFactory.class);
        final var stream = factory.transform(context, testStream);

        final var context2 = createContext("name_score_keyed_process");
        final var factory2 =
                TransformFactory.findFactory(ProcessTransformFactory.IDENTITY)
                        .cast(ProcessTransformFactory.class);
        execAndAssert(
                factory2.transform(context2, stream),
                result -> {
                    assertName(result, "aaaa", "aaab", "aaaa", "aaab");
                    assertScore(result, 1, 2, 4, 6);
                });
    }
}
