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

import name.zicat.astatine.streaming.sql.parser.transform.ProjectTransformFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformFactory;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.junit.Assert;
import org.junit.Test;

/** ProjectTransformFactoryTest. */
public class ProjectTransformFactoryTest extends TransformFactoryTestBase {
    @SuppressWarnings("unchecked")
    @Test
    public void test() throws Exception {

        final var configuration = new Configuration();
        configuration.set(ProjectTransformFactory.OPTION_PROJECT_FIELD_INDEXES, " 1, 3 ");
        final var context = createContext(configuration);

        final var testStream =
                env.fromElements(new Tuple4<>("aaaa", 1, 2, 3), new Tuple4<>("aaab", 4, 5, 6));
        final var factory =
                TransformFactory.findFactory(ProjectTransformFactory.IDENTITY)
                        .cast(ProjectTransformFactory.class);
        execAndAssert(
                factory.transform(context, testStream),
                result -> {
                    Assert.assertEquals(2, result.size());
                    Assert.assertEquals((Integer) 1, ((Tuple2<Integer, Integer>) result.get(0)).f0);
                    Assert.assertEquals((Integer) 3, ((Tuple2<Integer, Integer>) result.get(0)).f1);
                    Assert.assertEquals((Integer) 4, ((Tuple2<Integer, Integer>) result.get(1)).f0);
                    Assert.assertEquals((Integer) 6, ((Tuple2<Integer, Integer>) result.get(1)).f1);
                });
    }
}
