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

package name.zicat.astatine.streaming.sql.runtime.process;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.heap.AbstractHeapState;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static name.zicat.astatine.streaming.sql.runtime.utils.ProcessUtils.addRowDataInListStateAndRegisterTimer;
import static name.zicat.astatine.streaming.sql.runtime.utils.ProcessUtils.filterProcessableData;
import static name.zicat.astatine.streaming.sql.runtime.utils.StateUtils.registerTimer;

/** DisorderDiscardFunction. */
public class DisorderDiscardFunction extends KeyedProcessFunction<RowData, RowData, RowData> {

  private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";

  protected final RowData.FieldGetter eventTimeGetter;
  protected final InternalTypeInfo<RowData> rowTypeInfo;

  protected transient MapState<Long, List<RowData>> valueState;
  protected transient ValueState<Long> registeredTimer;
  protected transient Counter dropCounter;
  protected transient boolean isHeapBackend;
  protected transient TypeSerializer<RowData> inputStateRowSerializer;

  public DisorderDiscardFunction(
      RowData.FieldGetter eventTimeGetter, InternalTypeInfo<RowData> rowTypeInfo) {
    this.eventTimeGetter = eventTimeGetter;
    this.rowTypeInfo = rowTypeInfo;
  }

  @SuppressWarnings("deprecation")
  @Override
  public void open(Configuration parameters) {
    final var context = getRuntimeContext();
    valueState =
        context.getMapState(
            new MapStateDescriptor<>("rowState", Types.LONG, new ListTypeInfo<>(rowTypeInfo)));
    registeredTimer = context.getState(new ValueStateDescriptor<>("registerTime", Types.LONG));
    dropCounter = context.getMetricGroup().counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
    isHeapBackend = (registeredTimer instanceof AbstractHeapState);
    final var objectReuseEnabled = getRuntimeContext().isObjectReuseEnabled();
    if (isHeapBackend && objectReuseEnabled) {
      inputStateRowSerializer =
          rowTypeInfo.createSerializer(
              getRuntimeContext().getExecutionConfig().getSerializerConfig());
    }
  }

  @Override
  public void processElement(
      RowData rowData,
      KeyedProcessFunction<RowData, RowData, RowData>.Context context,
      Collector<RowData> collector)
      throws Exception {
    if (inputStateRowSerializer != null) {
      rowData = inputStateRowSerializer.copy(rowData);
    }
    if (!addRowDataInListStateAndRegisterTimer(
        eventTimeGetter,
        rowData,
        valueState,
        registeredTimer,
        context.timerService(),
        true,
        isHeapBackend)) {
      dropCounter.inc();
    }
  }

  @Override
  public void onTimer(
      long timestamp,
      KeyedProcessFunction<RowData, RowData, RowData>.OnTimerContext ctx,
      Collector<RowData> out)
      throws Exception {

    final var timerService = ctx.timerService();
    final var currentWatermark = timerService.currentWatermark();
    final var processableData = new ArrayList<Map.Entry<Long, List<RowData>>>();
    final var lastUnprocessedTime =
        filterProcessableData(valueState, currentWatermark, processableData::add);
    processableData.sort(Map.Entry.comparingByKey());
    for (var entry : processableData) {
      for (var row : entry.getValue()) {
        out.collect(row);
      }
    }
    if (lastUnprocessedTime < Long.MAX_VALUE) {
      registerTimer(registeredTimer, lastUnprocessedTime, timerService);
    } else {
      registeredTimer.clear();
      valueState.clear();
    }
  }
}
