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

package name.zicat.astatine.sql.client.test.parser;

import name.zicat.astatine.sql.client.parser.SettingSqlIterator;
import name.zicat.astatine.sql.client.parser.SqlIterator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

/** SettingSqlIteratorTest. */
public class SettingSqlIteratorTest {

  @Test
  public void test() throws Exception {
    try (var it1 = new SqlIteratorMock(List.of("s1"))) {
      var it2 = new SettingSqlIterator(it1, List.of("s4"));
      Assert.assertTrue(it2.hasNext());
      Assert.assertEquals("s4", it2.next());
      Assert.assertTrue(it2.hasNext());
      Assert.assertEquals("s1", it2.next());
      Assert.assertFalse(it2.hasNext());
      it2.close();
    }
  }

  public static final class SqlIteratorMock implements SqlIterator {

    private final Iterator<String> it;

    public SqlIteratorMock(List<String> sqlList) {
      this.it = sqlList.listIterator();
    }

    @Override
    public void close() {}

    @Override
    public boolean hasNext() {
      return it.hasNext();
    }

    @Override
    public String next() {
      return it.next();
    }
  }
}
