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

package name.zicat.astatine.sql.client.test.text;

import name.zicat.astatine.sql.client.text.SqlCommentReaderFilter;
import name.zicat.astatine.sql.client.text.TextReaderFilter;
import name.zicat.astatine.sql.client.util.StringUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;

/** SqlCommentReaderFilterTest. */
public class SqlCommentReaderFilterTest {
  @Test
  public void testFilter() throws Exception {
    final var filter = new SqlCommentReaderFilter();
    final var sql1 = "insert into aaa --test";
    Assert.assertEquals("insert into aaa", filter(filter, sql1));
    final var sql2 = "with ('connector' = 'aaa--bbb') -- test";
    Assert.assertEquals("with ('connector' = 'aaa--bbb')", filter(filter, sql2));
  }

  /**
   * filter s.
   *
   * @param filter filter
   * @param s s
   * @return new s
   * @throws Exception Exception
   */
  public static String filter(TextReaderFilter filter, String s) throws Exception {
    return StringUtil.toString(filter.filter(new StringReader(s)));
  }
}
