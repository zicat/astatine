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

import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** TextReaderChain. */
public class TextReaderFilterChain {

  private final List<TextReaderFilter> readers;

  public TextReaderFilterChain(List<TextReaderFilter> readers) {
    this.readers = readers;
  }

  public Reader execute(Reader reader) throws Exception {
    if (reader == null || readers == null || readers.isEmpty()) {
      return reader;
    }
    var result = reader;
    for (TextReaderFilter filter : readers) {
      result = filter.filter(result);
    }
    return result;
  }

  /** builder. */
  public static class Builder {
    private final List<TextReaderFilter> readers = new ArrayList<>();

    public Builder addTextReaderFilter(TextReaderFilter... textReaderFilter) {
      readers.addAll(Arrays.asList(textReaderFilter));
      return this;
    }

    public TextReaderFilterChain build() {
      return new TextReaderFilterChain(readers);
    }
  }
}
