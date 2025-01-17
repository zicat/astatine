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

import name.zicat.astatine.streaming.sql.parser.function.ConnectFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformContext;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

/** NameScoreSexTsConnectFactionFactory. */
public class NameScoreSexTsConnectFactionFactory
        implements ConnectFunctionFactory<NameScoreTs, NameSexTs, NameSexScoreTs> {

    @Override
    public String identity() {
        return "name_score_ts_connect";
    }

    @Override
    public DataStream<NameSexScoreTs> createConnect(
            TransformContext context, ConnectedStreams<NameScoreTs, NameSexTs> connectedStream) {
        return connectedStream.process(
                new KeyedCoProcessFunction<String, NameScoreTs, NameSexTs, NameSexScoreTs>() {

                    private ValueState<NameSexTs> valueState;
                    private ListState<NameScoreTs> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        final var descriptor =
                                new ValueStateDescriptor<>(
                                        "valueState",
                                        TypeInformation.of(new TypeHint<NameSexTs>() {}));
                        valueState = getRuntimeContext().getState(descriptor);

                        final var descriptor2 =
                                new ListStateDescriptor<>(
                                        "listState",
                                        TypeInformation.of(new TypeHint<NameScoreTs>() {}));
                        listState = getRuntimeContext().getListState(descriptor2);
                    }

                    @Override
                    public void processElement1(
                            NameScoreTs nameScoreTs,
                            KeyedCoProcessFunction<String, NameScoreTs, NameSexTs, NameSexScoreTs>
                                            .Context
                                    context,
                            Collector<NameSexScoreTs> collector)
                            throws Exception {
                        listState.add(nameScoreTs);
                        context.timerService().registerEventTimeTimer(nameScoreTs.getTs());
                    }

                    @Override
                    public void processElement2(
                            NameSexTs nameSexTs,
                            KeyedCoProcessFunction<String, NameScoreTs, NameSexTs, NameSexScoreTs>
                                            .Context
                                    context,
                            Collector<NameSexScoreTs> collector)
                            throws Exception {
                        valueState.update(nameSexTs);
                        context.timerService().registerEventTimeTimer(nameSexTs.getTs());
                    }

                    @Override
                    public void onTimer(
                            long timestamp,
                            KeyedCoProcessFunction<String, NameScoreTs, NameSexTs, NameSexScoreTs>
                                            .OnTimerContext
                                    ctx,
                            Collector<NameSexScoreTs> out)
                            throws Exception {
                        var it = listState.get().iterator();
                        var nameScoreTsResult = new ArrayList<NameScoreTs>();
                        while (it.hasNext()) {
                            var value = it.next();
                            if (value.getTs() <= timestamp) {
                                nameScoreTsResult.add(value);
                                it.remove();
                            }
                        }
                        nameScoreTsResult.sort(Comparator.comparingLong(NameTs::getTs));
                        var nameSexTs = valueState.value();
                        nameScoreTsResult.forEach(
                                v ->
                                        out.collect(
                                                new NameSexScoreTs(
                                                        v.getName(),
                                                        nameSexTs.isMale(),
                                                        v.getScore(),
                                                        v.getTs())));
                    }
                });
    }
}
