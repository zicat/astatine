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

package name.zicat.astatine.functions.math;

import org.apache.curator.shaded.com.google.common.hash.HashFunction;
import org.apache.curator.shaded.com.google.common.hash.Hashing;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.nio.charset.StandardCharsets;

/** HashCode. */
public class HashCode extends ScalarFunction {

  private static final int DEFAULT_SEED = 12331233;
  private static final String PARAM_SEED = "hashcode.seed";

  private transient HashFunction hashFunction;

  @Override
  public void open(FunctionContext context) throws Exception {
    super.open(context);
    hashFunction =
        Hashing.murmur3_128(
            Integer.parseInt(context.getJobParameter(PARAM_SEED, String.valueOf(DEFAULT_SEED))));
  }

  public long eval(String str) {
    if (str == null) {
      return 0;
    }
    return hashFunction.newHasher().putString(str, StandardCharsets.UTF_8).hash().asLong();
  }

  public long eval(Integer value) {
    if (value == null) {
      return 0;
    }
    return hashFunction.newHasher().putInt(value).hash().asLong();
  }

  public long eval(Long value) {
    if (value == null) {
      return 0;
    }
    return hashFunction.newHasher().putLong(value).hash().asLong();
  }

  public long eval(Short value) {
    if (value == null) {
      return 0;
    }
    return hashFunction.newHasher().putShort(value).hash().asLong();
  }

  public long eval(Float value) {
    if (value == null) {
      return 0;
    }
    return hashFunction.newHasher().putFloat(value).hash().asLong();
  }

  public long eval(Double value) {
    if (value == null) {
      return 0;
    }
    return hashFunction.newHasher().putDouble(value).hash().asLong();
  }
}
