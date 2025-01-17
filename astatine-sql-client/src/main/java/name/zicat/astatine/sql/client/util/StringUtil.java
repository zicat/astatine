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

package name.zicat.astatine.sql.client.util;

import java.io.IOException;
import java.io.Reader;

/** StringUtil. */
public class StringUtil {

  /**
   * reader to string builder.
   *
   * @param reader reader
   * @return string builder
   * @throws IOException IOException
   */
  public static StringBuilder toStringBuilder(Reader reader) throws IOException {
    final var sb = new StringBuilder();
    final var buf = new char[1024];
    int count;
    while ((count = reader.read(buf)) != -1) {
      sb.append(buf, 0, count);
    }
    return sb;
  }

  /**
   * reader to string.
   *
   * @param reader reader
   * @return string
   * @throws IOException IOException
   */
  public static String toString(Reader reader) throws IOException {
    return toStringBuilder(reader).toString();
  }
}
