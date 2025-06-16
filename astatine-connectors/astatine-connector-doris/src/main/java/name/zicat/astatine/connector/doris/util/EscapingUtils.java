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

package name.zicat.astatine.connector.doris.util;

import java.util.HashMap;
import java.util.Map;

/** EscapingUtils. */
public class EscapingUtils {

  private static final Map<Character, Character> ESCAPING_MAPPING = new HashMap<>();

  static {
    ESCAPING_MAPPING.put('t', '\t');
    ESCAPING_MAPPING.put('n', '\n');
    ESCAPING_MAPPING.put('b', '\b');
    ESCAPING_MAPPING.put('0', '\0');
    ESCAPING_MAPPING.put('f', '\f');
    ESCAPING_MAPPING.put('r', '\r');
    ESCAPING_MAPPING.put('\\', '\\');
  }

  /**
   * escaping str.
   *
   * @param value value
   * @return escaping result
   */
  public static String escaping(String value) {
    if (value == null) {
      return null;
    }
    final var sb = new StringBuilder();
    for (var i = 0; i < value.length(); i++) {
      var a = value.charAt(i);
      if (a == '\\') {
        if (i + 1 < value.length()) {
          var next = value.charAt(i + 1);
          var mapping = ESCAPING_MAPPING.get(next);
          if (mapping != null) {
            sb.append(mapping);
          } else {
            sb.append(a).append(next);
          }
          i = i + 1;
        } else {
          sb.append(a);
        }
      } else {
        sb.append(a);
      }
    }
    return sb.toString();
  }
}
