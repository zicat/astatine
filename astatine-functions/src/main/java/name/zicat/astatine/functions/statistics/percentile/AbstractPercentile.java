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

/** Percentile. */
public abstract class AbstractPercentile<T> extends AggregateFunction<T, AbstractPercentile.Acc> {

  private static final String DEFAULT_COMPRESSION_VALUE = "100";

  private double compression;

  @Override
  public void open(FunctionContext context) {
    compression =
        Double.parseDouble(context.getJobParameter(getCompressionKey(), DEFAULT_COMPRESSION_VALUE));
  }

  protected abstract String getCompressionKey();

  public void accumulate(Acc acc, Double iValue, double quantile) {
    if (iValue != null) {
      acc.addValue(iValue, quantile);
    }
  }

  public void accumulate(Acc acc, Integer iValue, double quantile) {
    if (iValue != null) {
      acc.addValue(iValue, quantile);
    }
  }

  public void accumulate(Acc acc, Short iValue, double quantile) {
    if (iValue != null) {
      acc.addValue(iValue, quantile);
    }
  }

  public void accumulate(Acc acc, Long iValue, double quantile) {
    if (iValue != null) {
      acc.addValue(iValue, quantile);
    }
  }

  public void accumulate(Acc acc, Float iValue, double quantile) {
    if (iValue != null) {
      acc.addValue(iValue, quantile);
    }
  }

  public void accumulate(Acc acc, MergingDigest digest) {
    acc.addValue(digest);
  }

  public void accumulate(Acc acc, MergingDigest digest, double quantile) {
    acc.addValue(digest, quantile);
  }

  public void accumulate(Acc acc, double iValue, double quantile) {
    acc.addValue(iValue, quantile);
  }

  public void accumulate(Acc acc, int iValue, double quantile) {
    acc.addValue(iValue, quantile);
  }

  public void accumulate(Acc acc, short iValue, double quantile) {
    acc.addValue(iValue, quantile);
  }

  public void accumulate(Acc acc, long iValue, double quantile) {
    acc.addValue(iValue, quantile);
  }

  public void accumulate(Acc acc, float iValue, double quantile) {
    acc.addValue(iValue, quantile);
  }

  protected Double getCompression() {
    return compression;
  }

  @Override
  public Acc createAccumulator() {
    return new Acc(getCompression());
  }

  /** acc. */
  public static class Acc {
    public MergingDigest tDigest;
    public double quantile;

    public Acc(double compression) {
      this.tDigest = new MergingDigest(compression);
    }

    public Acc() {}

    public Double getValue() {
      return tDigest.quantile(quantile);
    }

    public void addValue(double value, double quantile) {
      tDigest.add(value);
      this.quantile = quantile;
    }

    public void addValue(double value) {
      tDigest.add(value);
    }

    public void addValue(MergingDigest mergingDigest, double quantile) {
      tDigest.add(mergingDigest);
      this.quantile = quantile;
    }

    public void addValue(MergingDigest mergingDigest) {
      tDigest.add(mergingDigest);
    }
  }
}
