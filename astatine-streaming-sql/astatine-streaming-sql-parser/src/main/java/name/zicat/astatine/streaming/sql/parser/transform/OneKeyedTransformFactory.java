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

import name.zicat.astatine.streaming.sql.parser.function.OneKeyedTransformFunctionFactory;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;

/** OneKeyedTransformFactory. */
public abstract class OneKeyedTransformFactory extends OneTransformFactory {

  @Override
  public DataStream<?> transform(TransformContext context, DataStream<?> stream) {
    return transform(context, castAsKeyedstream(stream));
  }

  /**
   * cast stream as keyed stream.
   *
   * @param stream stream
   * @return KeyedStream
   */
  public static KeyedStream<?, ?> castAsKeyedstream(DataStream<?> stream) {
    if (stream instanceof KeyedStream<?, ?> keyedStream) {
      return keyedStream;
    }
    throw new RuntimeException("stream is not a keyed stream");
  }

  /**
   * transform stream by keyed stream.
   *
   * @param context context
   * @param keyedStream keyedStream
   * @return DataStream
   */
  @SuppressWarnings("unchecked")
  protected DataStream<?> transform(TransformContext context, KeyedStream<?, ?> keyedStream) {
    final var factory = functionFactory(context).cast(functionFactoryClass());
    return factory.transform(context, keyedStream);
  }

  @SuppressWarnings("rawtypes")
  @Override
  protected Class<? extends OneKeyedTransformFunctionFactory> functionFactoryClass() {
    return OneKeyedTransformFunctionFactory.class;
  }

  @Override
  public abstract String identity();
}
