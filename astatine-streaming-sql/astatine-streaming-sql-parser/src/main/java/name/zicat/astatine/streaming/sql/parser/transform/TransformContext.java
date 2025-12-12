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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;

import java.util.Map;
import java.util.Optional;

/** TransformContext. */
public class TransformContext implements ReadableConfig {

  protected final StreamTableEnvironmentImpl tEnv;
  protected final ReadableConfig config;

  public TransformContext(StreamTableEnvironmentImpl tEnv, ReadableConfig config) {
    this.tEnv = tEnv;
    this.config = config;
  }

  /**
   * get stream table env.
   *
   * @return StreamTableEnvironmentImpl
   */
  public StreamTableEnvironmentImpl streamTableEnvironmentImpl() {
    return tEnv;
  }

  /**
   * with config.
   *
   * @param config config
   * @return TransformContext
   */
  public TransformContext withConfig(ReadableConfig config) {
    return new TransformContext(tEnv, config);
  }

  @Override
  public <T> T get(ConfigOption<T> configOption) {
    return config.get(configOption);
  }

  @Override
  public <T> Optional<T> getOptional(ConfigOption<T> configOption) {
    return config.getOptional(configOption);
  }

  @Override
  public Map<String, String> toMap() {
    return config.toMap();
  }
}
