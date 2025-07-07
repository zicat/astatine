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

package name.zicat.astatine.streaming.sql.parser.test.transform;

import name.zicat.astatine.streaming.sql.parser.function.FunctionFactory;
import name.zicat.astatine.streaming.sql.parser.test.function.NameScore;
import name.zicat.astatine.streaming.sql.parser.test.function.WatermarkTestFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformFactory;
import name.zicat.astatine.streaming.sql.parser.transform.WatermarkTransformFactory;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.Iterator;

/** WatermarkTransformFactoryTest. */
public class WatermarkTransformFactoryTest extends TransformFactoryTestBase {

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void test() throws Exception {

    final var wk = 1000L;
    final var testStream =
        env.setParallelism(1).fromElements(new NameScore("aaaa", 1), new NameScore("aaab", 2));
    final var configuration = new Configuration();
    configuration.set(
        FunctionFactory.OPTION_FUNCTION_IDENTITY, WatermarkTestFunctionFactory.IDENTITY);
    configuration.set(WatermarkTestFunctionFactory.OPTION_WATERMARK_VALUE, wk);

    final var context = createContext(configuration);
    final var factory =
        TransformFactory.findFactory(WatermarkTransformFactory.IDENTITY)
            .cast(WatermarkTransformFactory.class);

    final Iterator<Long> expectedValuesIt = new ExpectValueIterator(wk, Long.MAX_VALUE);
    final var resultStream = factory.transform(context, testStream);
    resultStream.addSink(
        new SinkFunction() {

          @Override
          public void writeWatermark(Watermark watermark) {
            Assert.assertEquals(expectedValuesIt.next().longValue(), watermark.getTimestamp());
          }
        });
    resultStream.getExecutionEnvironment().execute();
  }

  /** ExpectValueIterator. */
  public static class ExpectValueIterator implements Iterator<Long>, Serializable {

    private final long[] value;
    private int offset = 0;

    public ExpectValueIterator(long... value) {
      this.value = value;
    }

    @Override
    public boolean hasNext() {
      return offset < value.length;
    }

    @Override
    public Long next() {
      final var id = offset;
      offset++;
      return value[id];
    }
  }
}
