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

package name.zicat.astatine.sql.client.register;

import name.zicat.astatine.sql.client.util.UnSupportedSqlException;

import name.zicat.astatine.streaming.sql.parser.parser.PlusSqlTableEnvironment;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;

/** PlusSqlRegister. */
public class PlusSqlRegister extends DefaultSqlRegister {

  protected final PlusSqlTableEnvironment plusTableEnv;

  protected PlusSqlRegister(StreamTableEnvironmentImpl tEnv) {
    super(tEnv);
    this.plusTableEnv = new PlusSqlTableEnvironment(tEnv);
  }

  @Override
  protected void registerSql(StatementSet statementSet, String sql) {
    try {
      super.registerSql(statementSet, sql);
    } catch (SqlParserException | UnSupportedSqlException e) {
      try {
        if (!plusTableEnv.executeSql(statementSet, sql)) {
          throw e;
        }
      } catch (Exception e2) {
        e.addSuppressed(e2);
        throw e;
      }
    }
  }
}
