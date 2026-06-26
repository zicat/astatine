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

/** PBFieldDispatcher. */
public class PBFieldDispatcher {

    private static final int DEFAULT_SIZE = 32;

    private final FieldBinding[] array;
    private final Map<Integer, FieldBinding> map;

    public PBFieldDispatcher(int size) {
        this.array = new FieldBinding[size];
        this.map = new HashMap<>();
    }

    public PBFieldDispatcher() {
        this(DEFAULT_SIZE);
    }

    public void add(int fieldNumber, int rowIndex, PBFieldSetter fieldSetter) {
        final var binding = new FieldBinding(rowIndex, fieldSetter);
        if (inArray(fieldNumber)) {
            array[fieldNumber] = binding;
        } else {
            map.put(fieldNumber, binding);
        }
    }

    public FieldBinding get(int fieldNumber) {
        if (inArray(fieldNumber)) {
            return array[fieldNumber];
        }
        return map.get(fieldNumber);
    }

    private boolean inArray(int fieldNumber) {
        return fieldNumber >= 0 && fieldNumber < array.length;
    }

    /** FieldBinding. */
    public record FieldBinding(int rowIndex, PBFieldSetter fieldSetter) {}
}
