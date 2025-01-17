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

package name.zicat.astatine.streaming.sql.parser.utils;

import java.util.ArrayList;
import java.util.ServiceLoader;
import java.util.StringJoiner;

/** SpiFactory. */
public interface SpiFactory {

    /**
     * factory identity.
     *
     * @return identity
     */
    String identity();

    /**
     * find source factory by id.
     *
     * @param identity identity
     * @param clazz clazz
     * @return SpiFactory
     */
    static <T extends SpiFactory> T findFactory(String identity, Class<T> clazz) {
        final ServiceLoader<T> loader = ServiceLoader.load(clazz);
        final var result = new ArrayList<T>();
        for (T factory : loader) {
            if (factory.identity().equals(identity)) {
                result.add(factory);
            }
        }
        if (result.isEmpty()) {
            throw new RuntimeException("identity " + identity + " not found, " + clazz);
        }
        if (result.size() > 1) {
            final var join = new StringJoiner(",");
            result.forEach(v -> join.add(v.getClass().getName()));
            throw new RuntimeException("identity " + identity + " found multi implement " + join);
        }
        return result.get(0);
    }
}
