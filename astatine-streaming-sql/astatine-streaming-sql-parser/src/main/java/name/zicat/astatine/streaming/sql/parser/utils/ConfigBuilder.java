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

import name.zicat.astatine.streaming.sql.parser.function.FunctionFactory;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;

import java.io.Serial;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** ConfigBuilder. */
public class ConfigBuilder {

  private final ReadableConfig readableConfig;
  private final Map<String, Object> newData = new HashMap<>();

  private ConfigBuilder(ReadableConfig readableConfig) {
    this.readableConfig = readableConfig;
  }

  public static ConfigBuilder newBuilder(ReadableConfig readableConfig) {
    return new ConfigBuilder(readableConfig);
  }

  /**
   * put new key value.
   *
   * @param key key
   * @param value value
   * @return ReadableConfigBuilder
   */
  public ConfigBuilder putNewKeyValue(String key, Object value) {
    newData.put(key, value);
    return this;
  }

  /**
   * put new function identity.
   *
   * @param identity identity
   * @return ReadableConfigBuilder
   */
  public ConfigBuilder functionIdentity(String identity) {
    return putNewKeyValue(FunctionFactory.OPTION_FUNCTION_IDENTITY.key(), identity);
  }

  /**
   * get readable config.
   *
   * @return readable config
   */
  public ReadableConfig build() {
    return new ReadableConfigWrapper(readableConfig, newData);
  }

  /** ReadableConfigWrapper. */
  private static final class ReadableConfigWrapper implements ReadableConfig, Serializable {
    @Serial private static final long serialVersionUID = 0L;
    private final ReadableConfig readableConfig;
    private final Map<String, Object> newData;

    /**
     * @param readableConfig readableConfig
     * @param newData newData
     */
    private ReadableConfigWrapper(ReadableConfig readableConfig, Map<String, Object> newData) {
      this.readableConfig = readableConfig;
      this.newData = newData;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(ConfigOption<T> configOption) {
      if (newData.containsKey(configOption.key())) {
        return (T) newData.get(configOption.key());
      }
      return readableConfig.get(configOption);
    }

    @Override
    public <T> Optional<T> getOptional(ConfigOption<T> configOption) {
      if (newData.containsKey(configOption.key())) {
        return Optional.of(get(configOption));
      }
      return readableConfig.getOptional(configOption);
    }

    @Override
    public Map<String, String> toMap() {
      final Map<String, String> result = readableConfig.toMap();
      newData.forEach((k, v) -> result.put(k, String.valueOf(v)));
      return result;
    }

    public ReadableConfig readableConfig() {
      return readableConfig;
    }

    public Map<String, Object> newData() {
      return newData;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj == null || obj.getClass() != this.getClass()) {
        return false;
      }
      var that = (ReadableConfigWrapper) obj;
      return Objects.equals(this.readableConfig, that.readableConfig)
          && Objects.equals(this.newData, that.newData);
    }

    @Override
    public int hashCode() {
      return Objects.hash(readableConfig, newData);
    }

    @Override
    public String toString() {
      return "ReadableConfigWrapper["
          + "readableConfig="
          + readableConfig
          + ", "
          + "newData="
          + newData
          + ']';
    }
  }
}
