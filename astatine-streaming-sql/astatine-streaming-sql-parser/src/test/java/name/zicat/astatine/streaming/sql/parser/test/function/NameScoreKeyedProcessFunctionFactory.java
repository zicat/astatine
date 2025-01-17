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

import name.zicat.astatine.streaming.sql.parser.function.KeyedProcessFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformContext;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/** NameScoreKeyProcessFunctionFactory. */
public class NameScoreKeyedProcessFunctionFactory
        implements KeyedProcessFunctionFactory<String, NameScore, NameScore> {

    @Override
    public KeyedProcessFunction<String, NameScore, NameScore> createKeyedProcess(
            TransformContext context) {
        return new KeyedProcessFunction<>() {

            private transient Map<String, NameScore> cache;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                cache = new HashMap<>();
            }

            @Override
            public void processElement(
                    NameScore nameScore,
                    KeyedProcessFunction<String, NameScore, NameScore>.Context context,
                    Collector<NameScore> collector) {
                final var nameScoreCache = cache.get(context.getCurrentKey());
                if (nameScoreCache == null) {
                    cache.put(context.getCurrentKey(), nameScore);
                    collector.collect(nameScore);
                } else {
                    final var newNameScore =
                            new NameScore(
                                    context.getCurrentKey(),
                                    nameScoreCache.getScore() + nameScore.getScore());
                    cache.put(context.getCurrentKey(), newNameScore);
                    collector.collect(newNameScore);
                }
            }
        };
    }

    @Override
    public TypeInformation<NameScore> returns() {
        return TypeInformation.of(NameScore.class);
    }

    @Override
    public String identity() {
        return "name_score_keyed_process";
    }
}
