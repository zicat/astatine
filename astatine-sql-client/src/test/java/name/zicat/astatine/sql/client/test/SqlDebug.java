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

package name.zicat.astatine.sql.client.test;

import static name.zicat.astatine.sql.client.SqlClient.createStatementSet;

import name.zicat.astatine.sql.client.register.PlusSqlRegisterFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.FileReader;
import java.util.TimeZone;

/** SqlDebug. */
public class SqlDebug {

  public static void main(String[] args) throws Exception {
    /*
       add vm param: --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-exports java.base/sun.net.util=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED
    */
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    final var reader = new FileReader("astatine-sql/sql/local/test_session_window.sql");
    createStatementSet(
            new PlusSqlRegisterFactory(
                StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1)),
            reader)
        .execute();
  }
}
