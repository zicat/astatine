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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.TimerService;

import java.io.IOException;

/** StateUtils. */
public class StateUtils {

  /**
   * register eventtime cleanup timer.
   *
   * @param timerService timerService
   * @param cleanupTimeState cleanupTimeState
   * @param minRetentionTime minRetentionTime
   * @param maxRetentionTime maxRetentionTime
   * @throws Exception Exception
   */
  public static void registerEventCleanupTimer(
      long lastUnprocessedTime,
      TimerService timerService,
      ValueState<Long> cleanupTimeState,
      long minRetentionTime,
      long maxRetentionTime)
      throws Exception {
    final var curCleanupTime = cleanupTimeState.value();
    if (curCleanupTime == null || (lastUnprocessedTime + minRetentionTime) > curCleanupTime) {
      if (curCleanupTime != null) {
        timerService.deleteEventTimeTimer(curCleanupTime);
      }
      final var cleanupTime = lastUnprocessedTime + maxRetentionTime;
      timerService.registerEventTimeTimer(cleanupTime);
      cleanupTimeState.update(cleanupTime);
    }
  }

  /**
   * register smallest timer.
   *
   * @param registeredTimer registeredTimer
   * @param timestamp timestamp
   * @param timerService timerService
   * @throws IOException IOException
   */
  public static void registerSmallestTimer(
      ValueState<Long> registeredTimer, long timestamp, TimerService timerService)
      throws IOException {
    Long currentRegisteredTimer = registeredTimer.value();
    if (currentRegisteredTimer == null) {
      registerTimer(registeredTimer, timestamp, timerService);
    } else if (currentRegisteredTimer > timestamp) {
      timerService.deleteEventTimeTimer(currentRegisteredTimer);
      registerTimer(registeredTimer, timestamp, timerService);
    }
  }

  /**
   * register timer.
   *
   * @param registeredTimer registeredTimer
   * @param timestamp timestamp
   * @param timerService timerService
   * @throws IOException IOException
   */
  public static void registerTimer(
      ValueState<Long> registeredTimer, long timestamp, TimerService timerService)
      throws IOException {
    registeredTimer.update(timestamp);
    timerService.registerEventTimeTimer(timestamp);
  }
}
