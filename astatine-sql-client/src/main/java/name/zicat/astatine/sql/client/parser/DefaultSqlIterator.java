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

package name.zicat.astatine.sql.client.parser;

import name.zicat.astatine.sql.client.util.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

/** DefaultSqlIterator. */
public class DefaultSqlIterator implements SqlIterator {
  private final BufferedReader reader;
  private String nextSql;

  public DefaultSqlIterator(Reader reader) {
    if (reader == null) {
      throw new NullPointerException("reader is null");
    }
    this.reader = new BufferedReader(reader);
  }

  @Override
  public boolean hasNext() {
    if (nextSql != null) {
      return true;
    }
    readNext();
    return nextSql != null;
  }

  @Override
  public String next() {
    if (nextSql == null) {
      throw new RuntimeException("not found");
    }
    final String sql = nextSql;
    nextSql = null;
    return sql;
  }

  /** read next. */
  private void readNext() {

    final var sb = new StringBuilder();
    String line;
    while ((line = readLine()) != null) {
      final var indexColon = findCharNotInApostrophe(";", line);
      if (indexColon >= 0) {
        sb.append(line, 0, indexColon);
        nextSql = sb.toString().trim();
        return;
      } else {
        sb.append(line);
        sb.append(System.lineSeparator());
      }
    }
  }

  /**
   * read line.
   *
   * @return string line
   */
  private String readLine() {
    try {
      return reader.readLine();
    } catch (IOException e) {
      throw new RuntimeException("read line fail", e);
    }
  }

  /**
   * @param cha cha
   * @param line line
   * @return -1 if not find
   */
  public static int findCharNotInApostrophe(String cha, String line) {
    var hasLeft = false;
    for (int i = 0; i < line.length(); i++) {
      if (i + cha.length() > line.length()) {
        break;
      }
      if (line.charAt(i) == '\'') {
        hasLeft = !hasLeft;
        continue;
      }
      if (line.indexOf(cha, i) == i && !hasLeft) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(reader);
  }
}
