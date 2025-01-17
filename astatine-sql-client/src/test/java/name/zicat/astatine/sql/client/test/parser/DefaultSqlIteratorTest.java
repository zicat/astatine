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

import name.zicat.astatine.sql.client.parser.DefaultSqlIterator;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;

/** DefaultSqlIteratorTest. */
public class DefaultSqlIteratorTest {

  @Test
  public void testFindCharNotInApostrophe() {
    final var s1 =
        """
               ';'';'
               ';;;'""";
    Assert.assertEquals(-1, DefaultSqlIterator.findCharNotInApostrophe(";", s1));
    final var s2 =
        """
               ';'';'
               ';;;'
               ;--aaa""";
    Assert.assertEquals(
        "--aaa", s2.substring(DefaultSqlIterator.findCharNotInApostrophe(";", s2) + 1));
  }

  @Test
  public void test() {
    final var s1 =
        """
            create java udf aaa ;
            create table bb with('connector' = 'bb;aa;ddd');
            create table c""";
    var sqlIterator = new DefaultSqlIterator(new StringReader(s1));
    Assert.assertTrue(sqlIterator.hasNext());
    Assert.assertEquals("create java udf aaa", sqlIterator.next());
    Assert.assertTrue(sqlIterator.hasNext());
    Assert.assertEquals("create table bb with('connector' = 'bb;aa;ddd')", sqlIterator.next());
    Assert.assertFalse(sqlIterator.hasNext());
  }
}
