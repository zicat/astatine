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

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.IntStream;
import org.antlr.v4.runtime.misc.Interval;

/** UpperCaseCharStream. */
public class UpperCaseCharStream implements CharStream {

  private final CodePointCharStream wrapper;

  public UpperCaseCharStream(CodePointCharStream wrapper) {
    this.wrapper = wrapper;
  }

  @Override
  public String getText(Interval interval) {
    return wrapper.getText(interval);
  }

  @Override
  public void consume() {
    wrapper.consume();
  }

  @Override
  public int LA(int i) {
    var la = wrapper.LA(i);
    if (la == 0 || la == IntStream.EOF) {
      return la;
    } else {
      return Character.toUpperCase(la);
    }
  }

  @Override
  public int mark() {
    return wrapper.mark();
  }

  @Override
  public void release(int marker) {
    wrapper.release(marker);
  }

  @Override
  public int index() {
    return wrapper.index();
  }

  @Override
  public void seek(int index) {
    wrapper.seek(index);
  }

  @Override
  public int size() {
    return wrapper.size();
  }

  @Override
  public String getSourceName() {
    return wrapper.getSourceName();
  }
}
