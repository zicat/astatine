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

import name.zicat.astatine.streaming.sql.parser.test.function.NameScoreTs;
import name.zicat.astatine.streaming.sql.parser.test.function.NameSexTs;
import name.zicat.astatine.streaming.sql.parser.test.function.NameTsKeySelect;
import name.zicat.astatine.streaming.sql.parser.transform.TransformFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TwoTransformFactory;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;

/** ConnectTransformFactoryTest. */
public class ConnectTransformFactoryTest extends TransformFactoryTestBase {

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void test() throws Exception {
        env.getConfig().setAutoWatermarkInterval(200);
        final var context = createContext("name_score_ts_connect");
        long ts = System.currentTimeMillis();
        final var leftStream = env.addSource(leftSource(ts)).keyBy(new NameTsKeySelect<>());
        final var rightStream = env.addSource(rightSource(ts)).keyBy(new NameTsKeySelect<>());

        final var factory =
                TransformFactory.findFactory(TwoTransformFactory.IDENTITY)
                        .cast(TwoTransformFactory.class);
        final var stream = factory.transform(context, leftStream, rightStream);
        execAndAssert(stream, (AssertHandler) result -> assertResult(ts, result));
    }

    public static<T> void assertResult(long ts, List<T> result) {
        result.sort(Comparator.comparing(Object::toString));
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(
                """
                            {name=aaaa, ts=%d, male=true, score=1}"""
                        .formatted(ts),
                result.get(0).toString());
        Assert.assertEquals(
                """
                            {name=aaab, ts=%d, male=false, score=2}"""
                        .formatted(ts),
                result.get(1).toString());
    }

    public static RichSourceFunction<NameSexTs> rightSource(long ts) {
        return new RichSourceFunction<>() {
            @Override
            public void run(SourceContext<NameSexTs> sourceContext) {
                sourceContext.emitWatermark(new Watermark(ts - 1));
                sourceContext.collectWithTimestamp(new NameSexTs("aaaa", true, ts), ts - 1);
                sourceContext.collectWithTimestamp(new NameSexTs("aaab", false, ts), ts - 1);
                sourceContext.emitWatermark(new Watermark(ts + 1000));
            }

            @Override
            public void cancel() {}
        };
    }

    public static RichSourceFunction<NameScoreTs> leftSource(long ts) {
        return new RichSourceFunction<>() {
            @Override
            public void run(SourceContext<NameScoreTs> sourceContext) {
                sourceContext.emitWatermark(new Watermark(ts - 1));
                sourceContext.collectWithTimestamp(new NameScoreTs("aaaa", 1, ts), ts - 1);
                sourceContext.collectWithTimestamp(new NameScoreTs("aaab", 2, ts), ts - 1);
                sourceContext.emitWatermark(new Watermark(ts + 1000));
            }

            @Override
            public void cancel() {}
        };
    }
}
