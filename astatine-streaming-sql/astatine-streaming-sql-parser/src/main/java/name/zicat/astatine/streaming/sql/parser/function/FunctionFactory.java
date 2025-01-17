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

package name.zicat.astatine.streaming.sql.parser.function;

import name.zicat.astatine.streaming.sql.parser.utils.SpiFactory;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/** FunctionFactory. */
public interface FunctionFactory extends SpiFactory {

  /** function identity config option. */
  ConfigOption<String> OPTION_FUNCTION_IDENTITY =
      ConfigOptions.key("identity").stringType().noDefaultValue();

  /**
   * find factory.
   *
   * @param identity identity
   * @return FunctionFactory
   */
  static FunctionFactory findFactory(String identity) {
    return SpiFactory.findFactory(identity, FunctionFactory.class);
  }

  /**
   * cast FunctionFactory as child type.
   *
   * @param clazz child class
   * @return child function factory
   * @param <T> type
   */
  default <T extends FunctionFactory> T cast(Class<T> clazz) {
    return clazz.cast(this);
  }

  /**
   * check is instance of clazz.
   *
   * @param clazz clazz
   * @return true if is.
   * @param <T> type
   */
  default <T extends FunctionFactory> boolean isInstanceOf(Class<T> clazz) {
    return clazz.isInstance(this);
  }

  /**
   * set returns.
   *
   * @param type type
   * @param stream stream
   * @return data stream
   * @param <T> T
   */
  default <T> DataStream<T> setReturn(
      TypeInformation<T> type, SingleOutputStreamOperator<T> stream) {
    return type == null ? stream : stream.returns(type);
  }
}
