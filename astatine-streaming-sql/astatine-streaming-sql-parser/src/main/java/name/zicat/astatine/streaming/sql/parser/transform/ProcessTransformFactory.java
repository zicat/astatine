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

import name.zicat.astatine.streaming.sql.parser.function.KeyedProcessFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.function.ProcessFunctionFactory;

import org.apache.flink.streaming.api.datastream.DataStream;

/** ProcessTransformFactory. */
public class ProcessTransformFactory extends OneTransformFactory {

  public static final String IDENTITY = "PROCESS";

  @Override
  public String identity() {
    return IDENTITY;
  }

  @SuppressWarnings("unchecked")
  @Override
  public DataStream<?> transform(TransformContext context, DataStream<?> stream) {
    final var functionFactory = functionFactory(context);
    if (functionFactory.isInstanceOf(ProcessFunctionFactory.class)) {
      return functionFactory.cast(ProcessFunctionFactory.class).transform(context, stream);
    } else if (functionFactory.isInstanceOf(KeyedProcessFunctionFactory.class)) {
      return functionFactory
          .cast(KeyedProcessFunctionFactory.class)
          .transform(context, OneKeyedTransformFactory.castAsKeyedstream(stream));
    } else {
      throw new RuntimeException("unknown process type " + functionFactory.getClass());
    }
  }
}
