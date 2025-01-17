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

import name.zicat.astatine.streaming.sql.parser.function.FilterFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformContext;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** NameScoreFilterFunctionFactory. */
public class NameScoreFilterFunctionFactory implements FilterFunctionFactory<NameScore> {

    public static final ConfigOption<String> OPTION_NAME_PREFIX =
            ConfigOptions.key("name_prefix").stringType().noDefaultValue();

    @Override
    public FilterFunction<NameScore> createFilter(TransformContext context) {
        final var prefix = context.get(OPTION_NAME_PREFIX);
        if (prefix == null) {
            throw new IllegalArgumentException("prefix is null");
        }
        return value -> value.getName().startsWith(prefix);
    }

    @Override
    public TypeInformation<NameScore> returns() {
        return TypeInformation.of(NameScore.class);
    }

    @Override
    public String identity() {
        return "name_score_filter";
    }
}
