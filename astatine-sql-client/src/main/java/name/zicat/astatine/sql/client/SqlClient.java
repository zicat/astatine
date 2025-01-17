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

package name.zicat.astatine.sql.client;

import name.zicat.astatine.sql.client.parser.DefaultSqlIterator;
import name.zicat.astatine.sql.client.parser.SettingSqlIterator;
import name.zicat.astatine.sql.client.register.PlusSqlRegisterFactory;
import name.zicat.astatine.sql.client.register.SqlRegisterFactory;
import name.zicat.astatine.sql.client.text.DefaultTemplateAddFilter;
import name.zicat.astatine.sql.client.text.FreemarkerReaderFilter;
import name.zicat.astatine.sql.client.text.SqlCommentReaderFilter;
import name.zicat.astatine.sql.client.text.TextReaderFilterChain;
import name.zicat.astatine.sql.client.util.FileUtil;

import org.apache.flink.table.api.StatementSet;

import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/** SqlClient. */
public class SqlClient {

  /**
   * execute sql file with setting.
   *
   * @param factory factory
   * @param reader reader
   * @param setSqlList setSqlList
   * @param envTemplate envTemplate
   * @throws Exception Exception
   */
  public static StatementSet createStatementSet(
          SqlRegisterFactory factory, Reader reader, List<String> setSqlList, String envTemplate) throws Exception {
    // remove sql comment and parse freemarker.
    final var filterReader =
            new TextReaderFilterChain.Builder()
                    .addTextReaderFilter(new SqlCommentReaderFilter())
                    .addTextReaderFilter(new DefaultTemplateAddFilter(envTemplate))
                    .addTextReaderFilter(new FreemarkerReaderFilter())
                    .build()
                    .execute(reader);
    final var it = new SettingSqlIterator(new DefaultSqlIterator(filterReader), setSqlList);
    return factory.createSqlRegister().register(it);
  }

  /**
   * execute sql file with setting.
   *
   * @param factory factory
   * @param reader reader
   * @param setSqlList setSqlList
   * @throws Exception Exception
   */
  public static StatementSet createStatementSet(
      SqlRegisterFactory factory, Reader reader, List<String> setSqlList) throws Exception {
    return createStatementSet(factory, reader, setSqlList, null);
  }

  /**
   * execute sql file with setting.
   *
   * @param factory factory
   * @param reader reader
   * @throws Exception Exception
   */
  public static StatementSet createStatementSet(SqlRegisterFactory factory, Reader reader)
      throws Exception {
    return createStatementSet(factory, reader, null);
  }

  public static void main(String[] args) throws Exception {
    final var sql = FileUtil.createReader(args[0], StandardCharsets.UTF_8);
    final var setting = args.length > 1 ? Arrays.asList(args).subList(1, args.length) : null;
    createStatementSet(new PlusSqlRegisterFactory(), sql, setting).execute();
  }
}
