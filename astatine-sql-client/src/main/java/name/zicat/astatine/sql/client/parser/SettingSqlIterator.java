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

import java.util.Iterator;
import java.util.List;

/** SettingSqlIterator. */
public class SettingSqlIterator implements SqlIterator {

  private final Iterator<String> it;
  private final SqlIterator wrapper;

  public SettingSqlIterator(SqlIterator wrapper, List<String> settings) {
    this.wrapper = wrapper;
    this.it = settings == null ? null : settings.listIterator();
  }

  @Override
  public boolean hasNext() {
    return it == null ? wrapper.hasNext() : it.hasNext() || wrapper.hasNext();
  }

  @Override
  public String next() {
    if (it != null && it.hasNext()) {
      return it.next();
    }
    return wrapper.next();
  }

  @Override
  public void close() throws Exception {
    wrapper.close();
  }
}
