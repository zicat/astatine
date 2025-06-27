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

import name.zicat.astatine.streaming.sql.parser.function.WatermarkFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformContext;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.io.Serializable;

/**
 * WatermarkTestFunctionFactory.
 *
 * @param <T>
 */
public class WatermarkTestFunctionFactory<T> implements WatermarkFunctionFactory<T> {

  public static final String IDENTITY = "watermark_test";
  public static final ConfigOption<Long> OPTION_WATERMARK_VALUE =
      ConfigOptions.key("watermark.value").longType().noDefaultValue();

  @Override
  public WatermarkStrategy<T> createWatermarkStrategy(TransformContext context) {
    return new TestWatermarkStrategy<>(context.get(OPTION_WATERMARK_VALUE));
  }

  @Override
  public String identity() {
    return IDENTITY;
  }

  /**
   * TestWatermarkStrategy.
   *
   * @param <T>
   */
  private static class TestWatermarkStrategy<T> implements WatermarkStrategy<T>, Serializable {

    private final long value;

    public TestWatermarkStrategy(long value) {
      this.value = value;
    }

    @Override
    public WatermarkGenerator<T> createWatermarkGenerator(
        WatermarkGeneratorSupplier.Context context) {
      return new WatermarkGenerator<>() {
        @Override
        public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
          output.emitWatermark(new Watermark(value));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {}
      };
    }
  }
}
