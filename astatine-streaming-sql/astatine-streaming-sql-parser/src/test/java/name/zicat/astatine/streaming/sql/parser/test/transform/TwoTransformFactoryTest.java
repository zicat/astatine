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
import name.zicat.astatine.streaming.sql.parser.test.function.NameScoreTs;
import name.zicat.astatine.streaming.sql.parser.test.function.NameSexScoreTs;
import name.zicat.astatine.streaming.sql.parser.test.function.NameSexTs;
import name.zicat.astatine.streaming.sql.parser.transform.TransformContext;
import name.zicat.astatine.streaming.sql.parser.transform.TransformFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TwoTransformFactory;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;

/** TwoTransformFactoryTest. */
public class TwoTransformFactoryTest extends TransformFactoryTestBase {

    @Test
    public void test() throws Exception {
        testByFunctionId(createContext(), "name_sex_score_transform");
    }

    /**
     * test by function id.
     *
     * @param id id
     * @throws Exception Exception
     */
    @SuppressWarnings("resource")
    public static void testByFunctionId(TransformContext context, String id) throws Exception {
        final var configuration = new Configuration();
        configuration.set(FunctionFactory.OPTION_FUNCTION_IDENTITY, id);

        final var env = context.streamTableEnvironmentImpl().execEnv();
        env.getConfig().setAutoWatermarkInterval(200);

        long ts = System.currentTimeMillis();
        final var leftStream = env.addSource(leftSource(ts));
        final var rightStream = env.addSource(rightSource(ts));

        final var factory =
                TransformFactory.findFactory(TwoTransformFactory.IDENTITY)
                        .cast(TwoTransformFactory.class);
        final var stream =
                factory.transform(context.withConfig(configuration), leftStream, rightStream);
        execAndAssert(stream, TwoTransformFactoryTest::assertResult);
    }

    public static RichSourceFunction<NameSexTs> rightSource(long ts) {
        return new RichSourceFunction<>() {
            @Override
            public void run(SourceContext<NameSexTs> sourceContext) {
                sourceContext.emitWatermark(
                        new org.apache.flink.streaming.api.watermark.Watermark(ts - 1));
                sourceContext.collectWithTimestamp(new NameSexTs("aaaa", true, ts), ts);
                sourceContext.collectWithTimestamp(new NameSexTs("aaab", false, ts), ts);
                sourceContext.collectWithTimestamp(
                        new NameSexTs("cccc", false, ts + 60000), ts + 60000);
            }

            @Override
            public void cancel() {}
        };
    }

    public static RichSourceFunction<NameScoreTs> leftSource(long ts) {
        return new RichSourceFunction<>() {
            @Override
            public void run(SourceContext<NameScoreTs> sourceContext) {
                sourceContext.emitWatermark(
                        new org.apache.flink.streaming.api.watermark.Watermark(ts - 1));
                sourceContext.collectWithTimestamp(new NameScoreTs("aaaa", 1, ts), ts);
                sourceContext.collectWithTimestamp(new NameScoreTs("aaab", 2, ts), ts);
                sourceContext.collectWithTimestamp(new NameScoreTs("aaaa", 3, ts), ts);
                sourceContext.collectWithTimestamp(new NameScoreTs("aaab", 4, ts), ts);
                sourceContext.collectWithTimestamp(
                        new NameScoreTs("cccc", 4, ts + 6000), ts + 6000);
            }

            @Override
            public void cancel() {}
        };
    }

    public static void assertResult(List<?> result) {
        Assert.assertEquals(4, result.size());
        final var list =
                result.stream()
                        .map(v -> (NameSexScoreTs) v)
                        .sorted(
                                Comparator.comparing((NameSexScoreTs o) -> o.getName())
                                        .thenComparingInt(NameSexScoreTs::getScore))
                        .toList();
        Assert.assertEquals("aaaa", list.get(0).getName());
        Assert.assertEquals("aaaa", list.get(1).getName());
        Assert.assertEquals("aaab", list.get(2).getName());
        Assert.assertEquals("aaab", list.get(3).getName());

        Assert.assertEquals(1, list.get(0).getScore());
        Assert.assertEquals(3, list.get(1).getScore());
        Assert.assertEquals(2, list.get(2).getScore());
        Assert.assertEquals(4, list.get(3).getScore());

        Assert.assertTrue(list.get(0).isMale());
        Assert.assertTrue(list.get(1).isMale());
        Assert.assertFalse(list.get(2).isMale());
        Assert.assertFalse(list.get(3).isMale());
    }
}
