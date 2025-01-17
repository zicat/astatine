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

package name.zicat.astatine.streaming.sql.parser.test.function;

import name.zicat.astatine.streaming.sql.parser.function.TwoTransformFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformContext;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

/** TwoTransformFunctionFactory. */
public class NameSexScoreTransformFunctionFactory
        implements TwoTransformFunctionFactory<
                DataStream<NameScoreTs>,
                DataStream<NameSexTs>,
                NameScoreTs,
                NameSexTs,
                NameSexScoreTs> {
    @Override
    public DataStream<NameSexScoreTs> transform(
            TransformContext context, DataStream<NameScoreTs> left, DataStream<NameSexTs> right) {
        return left.join(right)
                .where(new NameKeySelect<>())
                .equalTo(new NameKeySelect<>())
                .window(TumblingEventTimeWindows.of(Time.of(2, TimeUnit.SECONDS)))
                .apply(
                        (nameScore, nameSex) ->
                                new NameSexScoreTs(
                                        nameScore.getName(),
                                        nameSex.isMale(),
                                        nameScore.getScore(),
                                        nameSex.getTs()),
                        TypeInformation.of(NameSexScoreTs.class));
    }

    public static class NameKeySelect<T extends NameTs> implements KeySelector<T, String> {

        @Override
        public String getKey(T t) {
            return t.getName();
        }
    }

    @Override
    public String identity() {
        return "name_sex_score_transform";
    }
}
