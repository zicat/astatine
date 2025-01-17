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

import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.operations.UseCatalogOperation;
import org.apache.flink.table.operations.UseDatabaseOperation;
import org.apache.flink.table.operations.command.SetOperation;
import org.apache.flink.table.operations.ddl.CreateOperation;

import java.util.regex.Pattern;

/** AbstractSqlParser. */
public class DefaultSqlRegister extends AbstractSqlRegister {

  private final Pattern tableFunctionPattern =
      Pattern.compile(
          "(CREATE TEMPORARY TABLE FUNCTION\\s+)(.*)(\\s+KEY\\s+)(.*)"
              + "(\\s+TIMESTAMP\\s+)(.*)(\\s+FROM\\s+)(.*)",
          Pattern.CASE_INSENSITIVE);
  private static final String SET_JOB_PARAM_PREFIX = "jobparam.";
  private final TableEnvironmentInternal tEnv;
  private final Parser flinkSqlParser;

  public DefaultSqlRegister(TableEnvironmentInternal tEnv) {
    super(tEnv);
    this.tEnv = tEnv;
    this.flinkSqlParser = tEnv.getParser();
  }

  @Override
  protected void registerSql(StatementSet statementSet, String sql) {
    if (registerTemporalTableFunction(sql)) {
      return;
    }
    final var operators = flinkSqlParser.parse(sql);
    if (operators.size() > 1) {
      throw new UnSupportedSqlException(sql);
    }
    final var operator = operators.get(0);
    if (operator instanceof SinkModifyOperation) {
      statementSet.addInsertSql(sql); // e.g. insert into xxx select * from xxx
    } else if (operator instanceof CreateOperation) {
      tEnv.executeSql(sql); // e.g. create table xxx, create function
    } else if (operator instanceof SetOperation setOperation) {
      processSetOperation(setOperation, createCallback(sql)); // e.g. set 'aaa' = 'bbb'
    } else if (operator instanceof UseCatalogOperation useCatalogOperation) {
      tEnv.useCatalog(useCatalogOperation.getCatalogName());
    } else if (operator instanceof UseDatabaseOperation databaseOperation) {
      tEnv.useDatabase(databaseOperation.getDatabaseName());
    } else {
      throw new UnSupportedSqlException(sql);
    }
  }

  /**
   * create temporal table function by sql.
   *
   * @param sql sql
   * @return boolean register success
   */
  protected boolean registerTemporalTableFunction(String sql) {
    final var matcher = tableFunctionPattern.matcher(sql);
    if (!matcher.find()) {
      return false;
    }
    // e.g. CREATE TEMPORARY TABLE FUNCTION dim_sid KEY s1 TIMESTAMP proctime FROM bbb
    tEnv.createTemporaryFunction(
        matcher.group(2),
        tEnv.from(matcher.group(8))
            .createTemporalTableFunction(
                ApiExpressionUtils.unresolvedRef(matcher.group(6)),
                ApiExpressionUtils.unresolvedRef(matcher.group(4))));
    return true;
  }

  /**
   * process set operation. sql like: set 'aaaa' = 'bbb', if key start with SET_JOB_PARAM_PREFIX,
   * register key value as job parameter like set 'jobparam.aaa' = 'bbb'
   *
   * @param setOperation setOperation
   */
  protected void processSetOperation(SetOperation setOperation, Runnable failCallback) {
    if (setOperation.getKey().isEmpty() || setOperation.getValue().isEmpty()) {
      failCallback.run();
      return;
    }
    final var key = setOperation.getKey().get();
    final var value = setOperation.getValue().get();
    if (key.startsWith(SET_JOB_PARAM_PREFIX)) {
      tEnv.getConfig().addJobParameter(key.substring(SET_JOB_PARAM_PREFIX.length()), value);
    } else {
      tEnv.getConfig().set(key, value);
    }
  }

  /**
   * create unsupported sql callback.
   *
   * @param message message
   * @return runnable
   */
  public static Runnable createCallback(String message) {
    return () -> {
      throw new UnSupportedSqlException(message);
    };
  }
}
