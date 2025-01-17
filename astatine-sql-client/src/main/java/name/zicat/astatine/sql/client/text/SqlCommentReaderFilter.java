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

package name.zicat.astatine.sql.client.text;

import java.io.BufferedReader;
import java.io.Reader;
import java.io.StringReader;

/** SqlCommentReaderFilter. */
public class SqlCommentReaderFilter implements TextReaderFilter {

  public static final char COMMENT_CHAR = '-';

  @Override
  public Reader filter(Reader reader) throws Exception {

    final var br = new BufferedReader(reader);
    final var result = new StringBuilder();
    String line;
    boolean hasQuotation = false;
    while ((line = br.readLine()) != null) {
      line = line.trim();
      for (int i = 0; i < line.length(); i++) {
        final var c = line.charAt(i);
        if (c == '\'') {
          hasQuotation = !hasQuotation;
        } else if (c == COMMENT_CHAR
            && i < line.length() - 1
            && line.charAt(i + 1) == COMMENT_CHAR) {
          if (hasQuotation) {
            i++;
          } else {
            line = line.substring(0, i).trim();
            break;
          }
        }
      }
      if (!line.isEmpty()) {
        result.append(line);
        result.append(System.lineSeparator());
      }
    }
    return new StringReader(result.toString().trim());
  }
}
