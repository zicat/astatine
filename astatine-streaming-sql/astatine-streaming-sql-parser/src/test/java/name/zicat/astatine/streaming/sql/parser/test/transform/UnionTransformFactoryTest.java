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

import name.zicat.astatine.streaming.sql.parser.transform.TransformFactory;
import name.zicat.astatine.streaming.sql.parser.transform.UnionTransformFactory;

import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;

/** UnionTransformFactoryTest. */
public class UnionTransformFactoryTest extends TransformFactoryTestBase {

  @Test
  public void test() throws Exception {
    final var context = createContext();

    final var testStream = env.fromElements(1, 2);
    final var testStream2 = env.fromElements(3, 4);

    final var factory =
        TransformFactory.findFactory(UnionTransformFactory.IDENTITY)
            .cast(UnionTransformFactory.class);
    execAndAssert(
        factory.transform(context, testStream, testStream2),
        result -> {
          result.sort(Comparator.comparing(o -> ((Integer) o)));
          Assert.assertArrayEquals(new Integer[] {1, 2, 3, 4}, result.toArray());
        });

    try {
      execAndAssert(factory.transform(context, testStream), result -> {});
      Assert.fail("PairOperatorFactory only support two streams");
    } catch (Exception ignore) {

    }
  }
}
