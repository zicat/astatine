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
import name.zicat.astatine.streaming.sql.parser.test.function.NameScorePayload;
import name.zicat.astatine.streaming.sql.parser.transform.TransformContext;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.junit.Assert;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** TransformFactoryTestBase. */
public class TransformFactoryTestBase {

  protected StreamExecutionEnvironment env =
      StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

  public static void assertName(List<?> data, String... values) {
    for (int i = 0; i < data.size(); i++) {
      Assert.assertEquals(values[i], ((NameScore) data.get(i)).getName());
    }
  }

  public static void assertScore(List<?> data, Integer... values) {
    for (int i = 0; i < data.size(); i++) {
      Assert.assertEquals(values[i].intValue(), ((NameScore) data.get(i)).getScore());
    }
  }

  public static void assertPayload(List<?> data, String... values) {
    for (int i = 0; i < data.size(); i++) {
      Assert.assertEquals(values[i], ((NameScorePayload) data.get(i)).getPayload());
    }
  }

  @SuppressWarnings({"deprecation"})
  public static <T> void execAndAssert(DataStream<T> dataStream, AssertHandler<T> handler)
      throws Exception {
    dataStream.addSink(
        new SinkFunction<>() {

          private final List<T> result = new ArrayList<>();

          @Override
          public void invoke(T value) {
            result.add(value);
          }

          @Override
          public void finish() throws IOException {
            handler.assertResult(result);
          }
        });
    dataStream.getExecutionEnvironment().execute();
  }

  public TransformContext createContext(ReadableConfig config) {
    return new TransformContext(
        (StreamTableEnvironmentImpl) StreamTableEnvironment.create(env), config);
  }

  public TransformContext createContext() {
    return createContext(new Configuration());
  }

  /**
   * AssertHandler.
   *
   * @param <T> T
   */
  public interface AssertHandler<T> extends Serializable {
    void assertResult(List<T> result) throws IOException;
  }

  public TransformContext createContext(String functionIdentity) {
    final var configuration = new Configuration();
    configuration.set(FunctionFactory.OPTION_FUNCTION_IDENTITY, functionIdentity);
    return createContext(configuration);
  }
}
