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

package name.zicat.astatine.format.protobuf.parser;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.MapData;

/** GenericMapData2. */
public class GenericMapData2 implements MapData {

    private final Map<Object, Object> map;

    public GenericMapData2(Map<Object, Object> map) {
        this.map = map;
    }

    public GenericMapData2() {
        this(new HashMap<>());
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public ArrayData keyArray() {
        return new GenericArrayData(map.keySet().toArray());
    }

    @Override
    public ArrayData valueArray() {
        return new GenericArrayData(map.values().toArray());
    }

    public void put(Object key, Object value) {
        map.put(key, value);
    }

    public void putAll(Map<?, ?> values) {
        map.putAll(values);
    }

    public Map<?, ?> getMap() {
        return map;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof GenericMapData2 that)) {
            return false;
        }
        return deepEquals(map, that.map);
    }

    @Override
    public int hashCode() {
        int result = 0;
        for (Object key : map.keySet()) {
            result += 31 * Objects.hashCode(key);
        }
        return result;
    }

    private static <K, V> boolean deepEquals(Map<K, V> left, Map<?, ?> right) {
        if (left.size() != right.size()) {
            return false;
        }
        try {
            for (Map.Entry<K, V> entry : left.entrySet()) {
                K key = entry.getKey();
                V value = entry.getValue();
                if (value == null) {
                    if (!(right.get(key) == null && right.containsKey(key))) {
                        return false;
                    }
                } else if (!Objects.deepEquals(value, right.get(key))) {
                    return false;
                }
            }
        } catch (ClassCastException | NullPointerException ignored) {
            return false;
        }
        return true;
    }
}
