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

package name.zicat.astatine.streaming.sql.parser.transform;

import name.zicat.astatine.streaming.sql.parser.function.KeyByFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.function.RowDataKeyByFieldsSelectorFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.utils.ConfigBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;

import static name.zicat.astatine.streaming.sql.parser.function.FunctionFactory.OPTION_FUNCTION_IDENTITY;

/** KeyByTransformFactory. */
public class KeyByTransformFactory extends OneTransformFactory {

  public static final String IDENTITY = "KEY BY";

  @Override
  public String identity() {
    return IDENTITY;
  }

  @Override
  public DataStream<?> transform(TransformContext context, DataStream<?> stream) {
    final String id = context.get(OPTION_FUNCTION_IDENTITY);
    if (id == null) {
      context =
          context.withConfig(
              ConfigBuilder.newBuilder(context.config)
                  .put(OPTION_FUNCTION_IDENTITY, RowDataKeyByFieldsSelectorFunctionFactory.IDENTITY)
                  .build());
    }
    return super.transform(context, stream);
  }

  @SuppressWarnings("rawtypes")
  @Override
  protected Class<KeyByFunctionFactory> functionFactoryClass() {
    return KeyByFunctionFactory.class;
  }
}
