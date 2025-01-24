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

package name.zicat.astatine.streaming.sql.runtime.utils;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static name.zicat.astatine.streaming.sql.runtime.utils.StateUtils.registerSmallestTimer;

/** ProcessUtils. */
public class ProcessUtils {

  /** RowProcessFunction. */
  public interface ProcessFunction<T> {
    /**
     * process.
     *
     * @param entry entry
     * @throws Exception Exception
     */
    void process(Map.Entry<Long, T> entry) throws Exception;
  }

  /**
   * filter processable data.
   *
   * @param valueState valueState
   * @param currentWatermark currentWatermark
   * @param processFunction processFunction
   * @return last unprocessed time return max value if not found.
   * @param <T> Type
   * @throws Exception Exception
   */
  public static <T> long filterProcessableData(
      MapState<Long, T> valueState, long currentWatermark, ProcessFunction<T> processFunction)
      throws Exception {
    long lastUnprocessedTime = Long.MAX_VALUE;
    final var it = valueState.iterator();
    while (it.hasNext()) {
      final var entry = it.next();
      final var leftEventTime = entry.getKey();
      if (leftEventTime > currentWatermark) {
        lastUnprocessedTime = Math.min(lastUnprocessedTime, leftEventTime);
        continue;
      }
      it.remove();
      processFunction.process(entry);
    }
    return lastUnprocessedTime;
  }

  /**
   * add row data in state and register timer.
   *
   * @param eventTimeGetter eventTimeGetter
   * @param rowData rowData
   * @param valueState valueState
   * @param registeredTimer registeredTimer
   * @param timerService timerService
   * @throws Exception Exception
   */
  public static void addRowDataInListStateAndRegisterTimer(
      RowData.FieldGetter eventTimeGetter,
      RowData rowData,
      MapState<Long, List<RowData>> valueState,
      ValueState<Long> registeredTimer,
      TimerService timerService)
      throws Exception {
    addRowDataInListStateAndRegisterTimer(
        eventTimeGetter, rowData, valueState, registeredTimer, timerService, false);
  }

  /**
   * add row data in state and register timer.
   *
   * @param eventTimeGetter eventTimeGetter
   * @param rowData rowData
   * @param valueState valueState
   * @param registeredTimer registeredTimer
   * @param timerService timerService
   * @throws Exception Exception
   */
  public static void addRowDataInListStateAndRegisterTimer(
      RowData.FieldGetter eventTimeGetter,
      RowData rowData,
      MapState<Long, List<RowData>> valueState,
      ValueState<Long> registeredTimer,
      TimerService timerService,
      boolean discardDisorder)
      throws Exception {
    final var eventTime = eventTime(eventTimeGetter, rowData);
    if (discardDisorder && eventTime < timerService.currentWatermark()) {
      return;
    }
    var values = valueState.get(eventTime);
    if (values != null) {
      values.add(rowData);
      return;
    }
    values = new ArrayList<>();
    values.add(rowData);
    valueState.put(eventTime, values);
    registerSmallestTimer(registeredTimer, eventTime, timerService);
  }

  /**
   * get event time.
   *
   * @param eventTimeGetter eventTimeGetter
   * @param rowData rowData
   * @return long
   */
  public static long eventTime(RowData.FieldGetter eventTimeGetter, RowData rowData) {
    final var eventTime = (TimestampData) eventTimeGetter.getFieldOrNull(rowData);
    if (eventTime == null) {
      throw new RuntimeException("event time field is null");
    }
    return eventTime.getMillisecond();
  }
}
