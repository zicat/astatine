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

package name.zicat.astatine.functions.statistics.percentile;

import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;

/** PercentileTemporary. */
public class PercentileTemporary extends AggregateFunction<MergingDigest, AbstractPercentile.Acc> {

  private static final String COMPRESSION_KEY = "percentile.compression.temporary";
  private double compression;

  @Override
  public void open(FunctionContext context) {
    String compressionString = context.getJobParameter(COMPRESSION_KEY, "100");
    compression = Double.parseDouble(compressionString);
  }

  public void accumulate(AbstractPercentile.Acc acc, double iValue) {
    acc.addValue(iValue);
  }

  public void accumulate(AbstractPercentile.Acc acc, int iValue) {
    acc.addValue(iValue);
  }

  public void accumulate(AbstractPercentile.Acc acc, short iValue) {
    acc.addValue(iValue);
  }

  public void accumulate(AbstractPercentile.Acc acc, long iValue) {
    acc.addValue(iValue);
  }

  public void accumulate(AbstractPercentile.Acc acc, float iValue) {
    acc.addValue(iValue);
  }

  public void accumulate(AbstractPercentile.Acc acc, Double iValue) {
    if (iValue != null) {
      acc.addValue(iValue);
    }
  }

  public void accumulate(AbstractPercentile.Acc acc, Integer iValue) {
    if (iValue != null) {
      acc.addValue(iValue);
    }
  }

  public void accumulate(AbstractPercentile.Acc acc, Short iValue) {
    if (iValue != null) {
      acc.addValue(iValue);
    }
  }

  public void accumulate(AbstractPercentile.Acc acc, Long iValue) {
    if (iValue != null) {
      acc.addValue(iValue);
    }
  }

  public void accumulate(AbstractPercentile.Acc acc, Float iValue) {
    if (iValue != null) {
      acc.addValue(iValue);
    }
  }

  @Override
  public MergingDigest getValue(AbstractPercentile.Acc acc) {
    return acc.tDigest;
  }

  @Override
  public AbstractPercentile.Acc createAccumulator() {
    return new AbstractPercentile.Acc(compression);
  }
}
