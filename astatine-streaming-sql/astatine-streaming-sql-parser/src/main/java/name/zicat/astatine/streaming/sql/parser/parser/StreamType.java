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

package name.zicat.astatine.streaming.sql.parser.parser;

import name.zicat.astatine.streaming.sql.parser.extensions.PlusSqlExtensionsParser;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.StringJoiner;

/** StreamType. */
public enum StreamType {
  CREATE_STREAM("CREATE STREAM"),

  PRINT_FROM("PRINT FROM"),

  CREATE_VIEW("CREATE VIEW");

  private final String type;

  StreamType(String type) {
    this.type = type;
  }

  public String type() {
    return type;
  }

  /**
   * parser by statement.
   *
   * @param statement statement
   * @return StreamType
   */
  public static StreamType parse(PlusSqlExtensionsParser.StatementContext statement) {
    if (statement.CREATE() != null && statement.STREAM() != null) {
      return StreamType.parse(statement.CREATE(), statement.STREAM());
    } else if (statement.PRINT() != null && statement.FROM() != null) {
      return StreamType.parse(statement.PRINT(), statement.FROM());
    } else if (statement.CREATE() != null && statement.VIEW() != null) {
      return StreamType.parse(statement.CREATE(), statement.VIEW());
    } else {
      throw new RuntimeException("unknown stream type sql ");
    }
  }

  /**
   * parser.
   *
   * @param segments segments
   * @return StreamType
   */
  private static StreamType parse(ParseTree... segments) {
    final var join = new StringJoiner("_");
    for (var segment : segments) {
      join.add(segment.getText().toUpperCase());
    }
    return valueOf(join.toString());
  }
}
