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

import name.zicat.astatine.streaming.sql.parser.transform.TransformContext;

import org.apache.flink.streaming.api.datastream.DataStream;

/** TwoTransformFunctionFactory. */
public interface TwoTransformFunctionFactory<
        DS1 extends DataStream<T1>, DS2 extends DataStream<T2>, T1, T2, T3>
    extends FunctionFactory {

  /**
   * transform 2 data streams.
   *
   * @param context context
   * @param leftStream leftStream
   * @param rightStream rightStream
   * @return DataStream
   */
  DataStream<T3> transform(TransformContext context, DS1 leftStream, DS2 rightStream);
}
