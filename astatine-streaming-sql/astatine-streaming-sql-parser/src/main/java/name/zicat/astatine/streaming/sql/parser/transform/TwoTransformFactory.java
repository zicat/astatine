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

import name.zicat.astatine.streaming.sql.parser.function.TwoTransformFunctionFactory;

import org.apache.flink.streaming.api.datastream.DataStream;

/** TwoTransformFactory. */
public class TwoTransformFactory implements TransformFactory {

  public static final String IDENTITY = "TRANSFORM_WITH";

  @Override
  public DataStream<?> transform(TransformContext context, DataStream<?>... streams) {
    if (streams.length != 2) {
      throw new RuntimeException(getClass().getName() + " only support two input streams");
    }
    return transform(context, streams[0], streams[1]);
  }

  /**
   * transform stream by two stream.
   *
   * @param context context
   * @param leftStream leftStream
   * @param rightStream rightStream
   * @return DataStream
   */
  @SuppressWarnings("unchecked")
  protected DataStream<?> transform(
      TransformContext context, DataStream<?> leftStream, DataStream<?> rightStream) {
    return functionFactory(context)
        .cast(functionFactoryClass())
        .transform(context, leftStream, rightStream);
  }

  @SuppressWarnings("rawtypes")
  protected Class<? extends TwoTransformFunctionFactory> functionFactoryClass() {
    return TwoTransformFunctionFactory.class;
  }

  @Override
  public String identity() {
    return IDENTITY;
  }
}
