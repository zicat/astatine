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

import static org.apache.flink.table.api.config.ExecutionConfigOptions.IDLE_STATE_RETENTION;

import name.zicat.astatine.sql.client.SqlClient;
import name.zicat.astatine.sql.client.register.DefaultSqlRegisterFactory;
import name.zicat.astatine.sql.client.test.register.DefaultSqlRegisterTest;
import name.zicat.astatine.sql.client.test.udf.StringConcatUTest;

import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.api.internal.StatementSetImpl;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;

/** SqlClientTest. */
public class SqlClientTest {

  @Test
  public void testCreateStatement() throws Exception {

    final var factory = new DefaultSqlRegisterFactory();
    final var tEnv = factory.tableEnvironment();
    final var config = tEnv.getConfig();
    final var udfClazz = StringConcatUTest.class.getName();

    final var script =
        """
                create table bbb(v int, s1 STRING) WITH('connector' = 'datagen');
                create table aa(v int, S1 STRING) WITH('connector' = 'blackhole');
                CREATE TEMPORARY FUNCTION IF NOT EXISTS str_concat AS '%s' LANGUAGE JAVA;
                SET 'table.exec.state.ttl' = '120 s';
                SET 'jobparam.aa.bb' = '123';
                create view v1 as select v, str_concat(s1) from bbb where v <>0;
                insert into aa select * from v1;""";

    final var setting = Arrays.asList("SET 'bbb'='cccc'", "SET 'aaa'='bbb'");
    final StatementSetImpl<?> statementSet =
        (StatementSetImpl<?>)
            SqlClient.createStatementSet(
                factory, new StringReader(String.format(script, udfClazz)), setting, "env_utest");

    Assert.assertEquals("123", config.get(PipelineOptions.GLOBAL_JOB_PARAMETERS).get("aa.bb"));
    Assert.assertEquals(Duration.ofSeconds(120), config.get(IDLE_STATE_RETENTION));
    Assert.assertEquals("cccc", config.get(ConfigOptions.key("bbb").stringType().noDefaultValue()));
    Assert.assertEquals("bbb", config.get(ConfigOptions.key("aaa").stringType().noDefaultValue()));

    // check create table, create view
    final var catalogOpt = tEnv.getCatalog(tEnv.getCurrentCatalog());
    Assert.assertTrue(catalogOpt.isPresent());
    Assert.assertNotNull(catalogOpt.get().getTable(DefaultSqlRegisterTest.tableName(tEnv, "bbb")));
    Assert.assertNotNull(catalogOpt.get().getTable(DefaultSqlRegisterTest.tableName(tEnv, "aa")));
    Assert.assertNotNull(catalogOpt.get().getTable(DefaultSqlRegisterTest.tableName(tEnv, "v1")));

    // check create function
    Assert.assertTrue(Set.of(tEnv.listFunctions()).contains("str_concat"));

    // check insert
    Assert.assertEquals(1, statementSet.getOperations().size());
  }
}
