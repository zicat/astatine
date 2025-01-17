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

import name.zicat.astatine.streaming.sql.parser.function.IntervalJoinFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformContext;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/** NameSexScoreIntervalJoinFunctionFactory. */
public class NameSexScoreIntervalJoinFunctionFactory
        implements IntervalJoinFunctionFactory<String, NameScoreTs, NameSexTs, NameSexScoreTs> {
    @Override
    public DataStream<NameSexScoreTs> createIntervalJoinFunction(
            TransformContext context,
            KeyedStream.IntervalJoin<NameScoreTs, NameSexTs, String> intervalJoin) {
        return intervalJoin
                .between(Time.seconds(-1L), Time.seconds(1L))
                .process(new NameSexScoreIntervalJoinFunction());
    }

    @Override
    public String identity() {
        return "name_sex_score_interval_join";
    }

    private static class NameSexScoreIntervalJoinFunction
            extends ProcessJoinFunction<NameScoreTs, NameSexTs, NameSexScoreTs> {
        @Override
        public void processElement(
                NameScoreTs nameScoreTs,
                NameSexTs nameSexTs,
                ProcessJoinFunction<NameScoreTs, NameSexTs, NameSexScoreTs>.Context context,
                Collector<NameSexScoreTs> collector) {
            collector.collect(
                    new NameSexScoreTs(
                            nameScoreTs.getName(),
                            nameSexTs.isMale(),
                            nameScoreTs.getScore(),
                            nameScoreTs.getTs()));
        }
    }
}
