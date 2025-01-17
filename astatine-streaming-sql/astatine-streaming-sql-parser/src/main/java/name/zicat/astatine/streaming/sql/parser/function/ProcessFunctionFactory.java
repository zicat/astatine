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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;

/** ProcessFunctionFactory. */
public interface ProcessFunctionFactory<I, O>
    extends OneTransformFunctionFactory<DataStream<I>, I, O> {

  /**
   * crate process function.
   *
   * @param context context
   * @return ProcessFunction
   */
  ProcessFunction<I, O> createProcess(TransformContext context);

  /**
   * set returns.
   *
   * @return returns.
   */
  TypeInformation<O> returns();

  @Override
  default DataStream<O> transform(TransformContext context, DataStream<I> stream) {
    var result = stream.process(createProcess(context)).name(identity());
    return setReturn(returns(), result);
  }
}
