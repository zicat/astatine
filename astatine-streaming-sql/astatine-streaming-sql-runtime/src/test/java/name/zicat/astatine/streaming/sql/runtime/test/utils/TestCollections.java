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

package name.zicat.astatine.streaming.sql.runtime.test.utils;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/** TestCollections. */
public class TestCollections {

  public static <T> Iterator<T> serializableIterator(List<T> collection) {
    return new SerializableIterator<>(collection);
  }

  /**
   * SerializableIterator.
   *
   * @param <T>
   */
  private static class SerializableIterator<T> implements Serializable, Iterator<T> {

    private final List<T> collection;
    private int offset = 0;

    public SerializableIterator(List<T> collection) {
      this.collection = collection;
    }

    @Override
    public boolean hasNext() {
      return offset < collection.size();
    }

    @Override
    public T next() {
      final var nextOffset = offset + 1;
      T value = collection.get(offset);
      offset = nextOffset;
      return value;
    }
  }
}
