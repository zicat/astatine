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

package name.zicat.astatine.streaming.sql.parser.test.function;

import name.zicat.astatine.streaming.sql.parser.function.MapFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformContext;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

/** Row2NameScoreMapFunctionFactory. */
public class Row2NameScoreMapFunctionFactory implements MapFunctionFactory<Row, NameScore> {

    @Override
    public MapFunction<Row, NameScore> createMap(TransformContext context) {
        return row -> {
            final var v0 = (String) row.getField(0);
            final var v1 = row.getField(1);
            return new NameScore(v0, v1 == null ? 0 : (int) v1);
        };
    }

    @Override
    public TypeInformation<NameScore> returns() {
        return TypeInformation.of(NameScore.class);
    }

    @Override
    public String identity() {
        return "row_2_name_score";
    }
}
