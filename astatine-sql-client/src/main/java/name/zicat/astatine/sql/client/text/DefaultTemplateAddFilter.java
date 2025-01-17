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

/**
 * DefaultTemplateAddFilter.
 *
 * <p>This class add default template value by -Dtemplate.default if the reader not configuration
 * the template.
 *
 * <p>This class must add ahead of {@link FreemarkerReaderFilter}.
 *
 * <p>This class should add behind of {@link SqlCommentReaderFilter}.
 */
public class DefaultTemplateAddFilter implements TextReaderFilter {

  public static final String KEY_TEMPLATE_DEFAULT = "template.default";
  public static final String TEMPLATE_DEFAULT_VALUE = "env_local";
  private static final String TEMPLATE_START = "<#import ";
  private static final String TEMPLATE_END = "as template>";

  private final String envTemplate;

  public DefaultTemplateAddFilter() {
    this(null);
  }

  public DefaultTemplateAddFilter(String envTemplate) {
    this.envTemplate =
        envTemplate == null
            ? System.getProperty(KEY_TEMPLATE_DEFAULT, TEMPLATE_DEFAULT_VALUE)
            : envTemplate;
  }

  @Override
  public Reader filter(Reader reader) throws Exception {

    final var sb = new StringBuilder();
    final var br = new BufferedReader(reader);
    String line;
    var hasTemplateDefine = false;
    while ((line = br.readLine()) != null) {
      final var trimLine = line.trim();
      if (trimLine.startsWith(TEMPLATE_START) && trimLine.endsWith(TEMPLATE_END)) {
        hasTemplateDefine = true;
      }
      sb.append(line);
      sb.append(System.lineSeparator());
    }

    if (hasTemplateDefine) {
      return new StringReader(sb.toString());
    }
    final var template = TEMPLATE_START + "\"" + envTemplate + ".ftl\" " + TEMPLATE_END;
    return new StringReader(template + System.lineSeparator() + sb);
  }
}
