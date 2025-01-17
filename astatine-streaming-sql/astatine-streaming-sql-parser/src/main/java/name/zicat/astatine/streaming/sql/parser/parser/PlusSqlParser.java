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

import name.zicat.astatine.streaming.sql.parser.extensions.PlusSqlExtensionsLexer;
import name.zicat.astatine.streaming.sql.parser.extensions.PlusSqlExtensionsParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/** PlusSqlParser. */
public final class PlusSqlParser {

  /**
   * parse sql.
   *
   * @param sql sql
   * @return Operation
   */
  public StreamOperation parse(String sql) {

    final var parser = createParser(sql);
    final var statement = parser.statement();
    final var identifiers = statement.multipartIdentifier();
    if (identifiers.size() > 2) {
      final var str = identifiers.stream().map(Object::toString).collect(Collectors.joining(","));
      throw new RuntimeException("Plus sql only support 1 OR 2 identifiers, but got " + str);
    }

    final String name = identifiers.size() == 1 ? null : identifiers.get(0).getText();
    final String source =
        identifiers.size() == 1 ? identifiers.get(0).getText() : identifiers.get(1).getText();

    final var streamOperators = new ArrayList<StreamOperatorOperation>();
    for (var operator : statement.operators().operatorWithKeyValues()) {
      final var properties = new HashMap<String, String>();
      if (operator.keyValues() != null) {
        operator.keyValues().keyValue().forEach(keyValue -> addProperty(keyValue, properties));
      }
      final var operatorType =
          operator.operatorType() != null
              ? operator.operatorType().getText()
              : operator.operatorTypeWithOutKeyValues().getText();
      final var operatorSource =
          operator.multipartIdentifier() == null ? null : operator.multipartIdentifier().getText();
      streamOperators.add(new StreamOperatorOperation(operatorType, operatorSource, properties));
    }
    final var streamType = StreamType.parse(statement);

    final var nameProperties = new HashMap<String, String>();
    if (statement.nameIdentifierKeyValues() != null
        && statement.nameIdentifierKeyValues().keyValues() != null) {
      statement
          .nameIdentifierKeyValues()
          .keyValues()
          .keyValue()
          .forEach(keyValue -> addProperty(keyValue, nameProperties));
    }

    final var sourceProperties = new HashMap<String, String>();
    if (statement.sourceIdentifierKeyValues() != null
        && statement.sourceIdentifierKeyValues().keyValues() != null) {
      statement
          .sourceIdentifierKeyValues()
          .keyValues()
          .keyValue()
          .forEach(keyValue -> addProperty(keyValue, sourceProperties));
    }
    return new StreamOperation(
        streamType, name, source, streamOperators, sourceProperties, nameProperties);
  }

  /**
   * create parser by sql.
   *
   * @param sql sql
   * @return parser
   */
  private static PlusSqlExtensionsParser createParser(String sql) {
    final var stream = new UpperCaseCharStream(CharStreams.fromString(sql));
    final var lexer = new PlusSqlExtensionsLexer(stream);
    final var tokenStream = new CommonTokenStream(lexer);
    final var parser = new PlusSqlExtensionsParser(tokenStream);
    parser.addErrorListener(DefaultErrorListener.INSTANCE);
    return parser;
  }

  /**
   * add properties.
   *
   * @param keyValueContext keyValueContext
   * @param properties properties
   */
  private static void addProperty(
      PlusSqlExtensionsParser.KeyValueContext keyValueContext, Map<String, String> properties) {
    if (keyValueContext.constant().size() != 2) {
      throw new RuntimeException("parse properties fail " + keyValueContext.getText());
    }
    final var key = getConstant(keyValueContext.constant(0));
    final var value = getConstant(keyValueContext.constant(1));
    properties.put(
        key.replace("\\'", "'").replace("\\\"", "\""),
        value.replace("\\'", "'").replace("\\\"", "\""));
  }

  /**
   * remove '' of constant.
   *
   * @param constantContext constantContext
   * @return str
   */
  private static String getConstant(PlusSqlExtensionsParser.ConstantContext constantContext) {
    final var str = constantContext.getText();
    return str.substring(1, str.length() - 1);
  }
}
