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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class ViewUtils {

    private static final String VIEW_COMMON_PREFIX_NAME =
            "view_temporal_" + UUID.randomUUID().toString().substring(0, 8) + "_";
    private static final AtomicLong ID_GENERATOR = new AtomicLong(0);

    /**
     * create temporal view.
     *
     * @param tables tables
     * @return new view name
     */
    public static String createUniqueViewName(String[] tables) {
        while (true) {
            final var viewName = VIEW_COMMON_PREFIX_NAME + ID_GENERATOR.getAndIncrement();
            var contains = false;
            for (String table : tables) {
                if (table.equals(viewName)) {
                    contains = true;
                    break;
                }
            }
            if (!contains) {
                return viewName;
            }
        }
    }

    /**
     * table 2 stream.
     *
     * @param tEnv tEnv
     * @param table table
     * @return result
     */
    public static DataStream<RowData> table2Stream(StreamTableEnvironment tEnv, Table table) {
        return table2Stream(tEnv, table, RowData.class);
    }

    /**
     * table 2 stream.
     *
     * @param tEnv tEnv
     * @param table table
     * @param clazz class
     * @return result
     * @param <T> T
     */
    public static <T> DataStream<T> table2Stream(
            StreamTableEnvironment tEnv, Table table, Class<T> clazz) {
        return tEnv.toDataStream(
                table, table.getResolvedSchema().toSourceRowDataType().bridgedTo(clazz));
    }
}
