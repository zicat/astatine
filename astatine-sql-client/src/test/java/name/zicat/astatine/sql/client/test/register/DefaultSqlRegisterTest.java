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

package name.zicat.astatine.sql.client.test.register;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.IDLE_STATE_RETENTION;

import name.zicat.astatine.sql.client.register.DefaultSqlRegisterFactory;
import name.zicat.astatine.sql.client.test.parser.SettingSqlIteratorTest;
import name.zicat.astatine.sql.client.test.udf.StringConcatUTest;

import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.StatementSetImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;

/** DefaultSqlRegisterTest. */
public class DefaultSqlRegisterTest {

  @Test
  public void testRegister() throws TableNotExistException, IOException {

    final var factory = new DefaultSqlRegisterFactory();
    final var tEnv = factory.tableEnvironment();
    final var config = tEnv.getConfig();
    final var className = StringConcatUTest.class.getName();
    final var register = factory.createSqlRegister();
    final var sqlList =
        new SettingSqlIteratorTest.SqlIteratorMock(
            Arrays.asList(
                "create table bbb(v int, s1 STRING, proctime AS proctime()) WITH('connector' = 'datagen')",
                "create table aa(v int, S1 STRING) WITH('connector' = 'blackhole')",
                "CREATE TEMPORARY FUNCTION IF NOT EXISTS str_concat AS '"
                    + className
                    + "' LANGUAGE JAVA",
                "CREATE TEMPORARY TABLE FUNCTION dim_sid KEY s1 TIMESTAMP proctime FROM bbb",
                "SET 'table.exec.state.ttl' = '120 s'",
                "SET 'jobparam.aa.bb' = '123'",
                "create view v1 as select v, str_concat(s1) from bbb where v <>0",
                "insert into aa select * from v1"));
    var statementSet = (StatementSetImpl<?>) register.register(sqlList);
    // check set
    Assert.assertEquals("123", config.get(PipelineOptions.GLOBAL_JOB_PARAMETERS).get("aa.bb"));
    Assert.assertEquals(Duration.ofSeconds(120), config.get(IDLE_STATE_RETENTION));

    // check create table, create view
    final var catalogOpt = tEnv.getCatalog(tEnv.getCurrentCatalog());
    Assert.assertTrue(catalogOpt.isPresent());
    Assert.assertNotNull(catalogOpt.get().getTable(tableName(tEnv, "bbb")));
    Assert.assertNotNull(catalogOpt.get().getTable(tableName(tEnv, "aa")));
    Assert.assertNotNull(catalogOpt.get().getTable(tableName(tEnv, "v1")));

    // check create function
    Assert.assertTrue(Set.of(tEnv.listFunctions()).contains("str_concat"));
    Assert.assertTrue(Set.of(tEnv.listFunctions()).contains("dim_sid"));

    // check insert
    Assert.assertEquals(1, statementSet.getOperations().size());
  }

  public static ObjectPath tableName(TableEnvironment tEnv, String tableName) {
    return new ObjectPath(tEnv.getCurrentDatabase(), tableName);
  }
}
