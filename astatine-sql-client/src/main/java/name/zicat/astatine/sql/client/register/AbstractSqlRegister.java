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

import name.zicat.astatine.sql.client.parser.SqlIterator;

import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** AbstractSqlRegister. */
public abstract class AbstractSqlRegister implements SqlRegister {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSqlRegister.class);
  private final TableEnvironment tEnv;

  protected AbstractSqlRegister(TableEnvironment tEnv) {
    this.tEnv = tEnv;
    setDefaultConfig(tEnv);
  }

  /**
   * set default tEnv.
   *
   * @param tEnv tEnv
   */
  protected void setDefaultConfig(TableEnvironment tEnv) {
    tEnv.getConfig().set(ExecutionOptions.SNAPSHOT_COMPRESSION, true);
  }

  /**
   * register single sql with statement set.
   *
   * @param statementSet statementSet
   * @param sql sql
   */
  protected abstract void registerSql(StatementSet statementSet, String sql);

  @Override
  public final StatementSet register(SqlIterator sqlIterator) {
    final var statementSet = tEnv.createStatementSet();
    while (sqlIterator.hasNext()) {
      final var sql = sqlIterator.next();
      LOG.info("start to register sql(p):\n{}", sql);
      registerSql(statementSet, sql);
    }
    return statementSet;
  }
}
