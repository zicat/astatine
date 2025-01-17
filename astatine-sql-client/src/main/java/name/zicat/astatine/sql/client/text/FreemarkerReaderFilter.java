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

import freemarker.cache.ClassTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/** FreemarkerReaderFilter. */
public class FreemarkerReaderFilter implements TextReaderFilter {

  protected final Configuration configuration = new Configuration(Configuration.VERSION_2_3_21);
  protected static final String BASE_PACKAGE_PATH = "/";

  public FreemarkerReaderFilter() {
    configuration.setTemplateLoader(
        new ClassTemplateLoader(Thread.currentThread().getContextClassLoader(), BASE_PACKAGE_PATH));
    configuration.setAutoFlush(false);
  }

  protected Charset charset() {
    return StandardCharsets.UTF_8;
  }

  @Override
  public Reader filter(Reader reader) throws Exception {
    final var template = new Template(null, reader, configuration);
    final var out = new StringWriter();
    template.process(null, out);
    final var stream = new ByteArrayInputStream(out.toString().getBytes(charset()));
    return new InputStreamReader(stream, charset());
  }
}
