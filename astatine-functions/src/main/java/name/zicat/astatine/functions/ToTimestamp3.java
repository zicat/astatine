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

package name.zicat.astatine.functions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

/** ToTimestamp3. */
public class ToTimestamp3 extends ScalarFunction {

  /**
   * long type to timestamp type.
   *
   * @param ts ts
   * @return timestamp
   */
  public @DataTypeHint(value = "TIMESTAMP(3)", bridgedTo = java.sql.Timestamp.class) Timestamp eval(
      Long ts) {
    return new Timestamp(ts);
  }

  /**
   * int type to timestamp type.
   *
   * @param ts ts
   * @return timestamp
   */
  public @DataTypeHint(value = "TIMESTAMP(3)", bridgedTo = java.sql.Timestamp.class) Timestamp eval(
      Integer ts) {
    return new Timestamp(ts * 1000L);
  }
}
