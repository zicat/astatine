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

import static name.zicat.astatine.streaming.sql.parser.utils.ConfigBuilder.newBuilder;

import name.zicat.astatine.streaming.sql.parser.function.aggregation.MinFunctionFactory;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;

/** MinTransformFactory. */
public class MinTransformFactory extends OneKeyedTransformFactory {

  public static final String IDENTITY = "MIN";

  @Override
  public String identity() {
    return IDENTITY;
  }

  @Override
  protected DataStream<?> transform(TransformContext context, KeyedStream<?, ?> keyedStream) {
    final var newConfig = newBuilder(context).functionIdentity(MinFunctionFactory.IDENTITY).build();
    return super.transform(context.withConfig(newConfig), keyedStream);
  }

  @SuppressWarnings("rawtypes")
  @Override
  protected Class<MinFunctionFactory> functionFactoryClass() {
    return MinFunctionFactory.class;
  }
}
