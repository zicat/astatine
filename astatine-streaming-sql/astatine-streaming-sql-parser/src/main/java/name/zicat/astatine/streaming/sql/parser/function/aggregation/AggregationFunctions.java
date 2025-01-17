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

package name.zicat.astatine.streaming.sql.parser.function.aggregation;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;

/** AggregationFunctions. */
public class AggregationFunctions {

  public static final ConfigOption<String> OPTION_AGGREGATION_FIELD =
      ConfigOptions.key("field").stringType().defaultValue("");
  public static final ConfigOption<Integer> OPTION_AGGREGATION_POSITION =
      ConfigOptions.key("position").intType().defaultValue(-1);

  public static final ConfigOption<Boolean> OPTION_AGGREGATION_FIRST =
      ConfigOptions.key("first").booleanType().defaultValue(true);

  /**
   * transform aggregation stream.
   *
   * @param config config
   * @param handler handler
   * @return DataStream
   */
  public static <O> DataStream<O> transformAggregation(
      ReadableConfig config, AggregationHandler<O> handler) {
    final var params = new AggregationParams(config);
    if (!OPTION_AGGREGATION_FIELD.defaultValue().equals(params.field)) {
      return handler.aggregationByField(params.field);
    } else if (!OPTION_AGGREGATION_POSITION.defaultValue().equals(params.position)) {
      return handler.aggregationByPosition(params.position);
    } else {
      throw new RuntimeException("aggregation operator must config field or position");
    }
  }

  /**
   * transform aggregation by stream.
   *
   * @param config config
   * @param handler handler
   * @return DataStream
   */
  public static <O> DataStream<O> transformAggregationBy(
      ReadableConfig config, AggregationByHandler<O> handler) {
    final var params = new AggregationByParams(config);
    if (!OPTION_AGGREGATION_FIELD.defaultValue().equals(params.field)) {
      return handler.aggregationByField(params.field, params.first);
    } else if (!OPTION_AGGREGATION_POSITION.defaultValue().equals(params.position)) {
      return handler.aggregationByPosition(params.position, params.first);
    } else {
      throw new RuntimeException("aggregation by operator must config field or position");
    }
  }

  /** AggregationHandler. */
  public interface AggregationHandler<O> {

    /**
     * aggregation by field.
     *
     * @param field field
     * @return DataStream
     */
    DataStream<O> aggregationByField(String field);

    /**
     * aggregation by position.
     *
     * @param position position
     * @return DataStream
     */
    DataStream<O> aggregationByPosition(int position);
  }

  /** AggregationByHandler. */
  public interface AggregationByHandler<O> {

    /**
     * aggregation by field.
     *
     * @param field field
     * @param first first
     * @return DataStream
     */
    DataStream<O> aggregationByField(String field, boolean first);

    /**
     * aggregation by position.
     *
     * @param position position
     * @param first first
     * @return DataStream
     */
    DataStream<O> aggregationByPosition(int position, boolean first);
  }

  /** AggregationByParams. */
  public static class AggregationByParams extends AggregationParams {

    private final boolean first;

    public AggregationByParams(ReadableConfig config) {
      super(config);
      this.first = config.get(OPTION_AGGREGATION_FIRST);
    }
  }

  /** AggregationParams. */
  public static class AggregationParams {

    protected final String field;
    protected final int position;

    public AggregationParams(ReadableConfig config) {
      this.field = config.get(OPTION_AGGREGATION_FIELD);
      this.position = config.get(OPTION_AGGREGATION_POSITION);
    }
  }
}
