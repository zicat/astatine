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

import name.zicat.astatine.streaming.sql.parser.function.OneKeyedTransformFunctionFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformContext;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;

/** MaxByFunctionFactory. */
public final class MaxByFunctionFactory<K, I> implements OneKeyedTransformFunctionFactory<K, I, I> {

  public static final String IDENTITY = "aggregation_max_by";

  @Override
  public TypeInformation<I> returns() {
    return null;
  }

  @Override
  public DataStream<I> transform(TransformContext context, KeyedStream<I, K> keyedStream) {
    return AggregationFunctions.transformAggregationBy(
        context,
        new AggregationFunctions.AggregationByHandler<>() {
          @Override
          public DataStream<I> aggregationByField(String field, boolean first) {
            return keyedStream.maxBy(field, first);
          }

          @Override
          public DataStream<I> aggregationByPosition(int position, boolean first) {
            return keyedStream.maxBy(position, first);
          }
        });
  }

  @Override
  public String identity() {
    return IDENTITY;
  }
}
