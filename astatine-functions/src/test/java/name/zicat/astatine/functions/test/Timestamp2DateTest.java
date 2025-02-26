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

package name.zicat.astatine.functions.test;

import name.zicat.astatine.functions.Timestamp2Date;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.LocalDate;

/** Timestamp2DateTest. */
public class Timestamp2DateTest {

  @Test
  public void test() {
    final var timestamp2Date = new Timestamp2Date();
    Assert.assertEquals(LocalDate.of(2025, 1, 1), timestamp2Date.eval(1735768800000L));
    Assert.assertEquals(LocalDate.of(2025, 1, 2), timestamp2Date.eval(1735768800000L, "GMT+8"));
    Assert.assertEquals(
        LocalDate.of(2025, 1, 1), timestamp2Date.eval(new Timestamp(1735768800000L)));
    Assert.assertEquals(
        LocalDate.of(2025, 1, 2), timestamp2Date.eval(new Timestamp(1735768800000L), "GMT+8"));
  }
}
