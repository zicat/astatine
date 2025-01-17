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

package name.zicat.astatine.sql.client.test.table;

import name.zicat.astatine.sql.client.table.DataHourPartitionTimeExtractor;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/** DataHourPartitionTimeExtractorTest. */
public class DataHourPartitionTimeExtractorTest {

  @Test
  public void test() {
    final var timeExtractor = new DataHourPartitionTimeExtractor();
    final var time = timeExtractor.extract(null, Arrays.asList("20231213", "12"));
    Assert.assertEquals(2023, time.getYear());
    Assert.assertEquals(12, time.getMonth().getValue());
    Assert.assertEquals(13, time.getDayOfMonth());
    Assert.assertEquals(12, time.getHour());
  }
}
