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

import static name.zicat.astatine.streaming.sql.parser.function.FunctionFactory.OPTION_FUNCTION_IDENTITY;

import name.zicat.astatine.streaming.sql.parser.function.FunctionFactory;
import name.zicat.astatine.streaming.sql.parser.utils.SpiFactory;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;

/** TransformFactory. */
public interface TransformFactory extends SpiFactory {

  /**
   * transform stream.
   *
   * @param context context
   * @param streams streams
   * @return stream
   */
  DataStream<?> transform(TransformContext context, DataStream<?>... streams);

  /**
   * get function factory.
   *
   * @param config config.
   * @return FunctionFactory
   */
  default FunctionFactory functionFactory(ReadableConfig config) {
    return FunctionFactory.findFactory(config.get(OPTION_FUNCTION_IDENTITY));
  }

  /**
   * find operator factory by identity.
   *
   * @param identity identity
   * @return OperatorFactory
   */
  static TransformFactory findFactory(String identity) {
    return SpiFactory.findFactory(identity, TransformFactory.class);
  }

  /**
   * cast operator factory.
   *
   * @param clazz clazz
   * @return factory
   * @param <T> T
   */
  default <T extends TransformFactory> T cast(Class<T> clazz) {
    return clazz.cast(this);
  }
}
