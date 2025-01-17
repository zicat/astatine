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

package name.zicat.astatine.sql.client.table;

import org.apache.flink.connector.file.table.PartitionTimeExtractor;

import java.time.LocalDateTime;
import java.util.List;

/** DataHourPartitionTimeExtractor. */
public class DataHourPartitionTimeExtractor implements PartitionTimeExtractor {

  public DataHourPartitionTimeExtractor() {}

  @Override
  public LocalDateTime extract(List<String> partitionKeys, List<String> partitionValues) {
    final var yearMonthData = partitionValues.get(0);
    final var hour = partitionValues.get(1);
    try {
      return LocalDateTime.of(
          Integer.parseInt(yearMonthData.substring(0, 4)),
          Integer.parseInt(yearMonthData.substring(4, 6)),
          Integer.parseInt(yearMonthData.substring(6, 8)),
          Integer.parseInt(hour),
          0,
          0);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to extract partition time, date=" + yearMonthData + ",hour=" + hour, e);
    }
  }
}
