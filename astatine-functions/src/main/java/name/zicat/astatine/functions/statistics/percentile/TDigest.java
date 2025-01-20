/*
 * Licensed to Ted Dunning under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package name.zicat.astatine.functions.statistics.percentile;

import java.io.Serializable;
import java.util.Collection;

/**
 * Adaptive histogram based on something like streaming k-means crossed with Q-digest.
 *
 * <p>The special characteristics of this algorithm are:
 *
 * <p>- smaller summaries than Q-digest
 *
 * <p>- works on doubles as well as integers.
 *
 * <p>- provides part per million accuracy for extreme quantiles and typically &lt;1000 ppm accuracy
 * for middle quantiles
 *
 * <p>- fast
 *
 * <p>- simple
 *
 * <p>- test coverage roughly at 90%
 *
 * <p>- easy to adapt for use with map-reduce
 */
public abstract class TDigest implements Serializable {
  public double min = Double.POSITIVE_INFINITY;
  public double max = Double.NEGATIVE_INFINITY;

  /**
   * Adds a sample to a histogram.
   *
   * @param x The value to add.
   * @param w The weight of this point.
   */
  public abstract void add(double x, int w);

  /**
   * Re-examines a t-digest to determine whether some centroids are redundant. If your data are
   * perversely ordered, this may be a good idea. Even if not, this may save 20% or so in space.
   *
   * <p>The cost is roughly the same as adding as many data points as there are centroids. This is
   * typically &lt; 10 * compression, but could be as high as 100 * compression.
   *
   * <p>This is a destructive operation that is not thread-safe.
   */
  public abstract void compress();

  /**
   * Returns the number of points that have been added to this TDigest.
   *
   * @return The sum of the weights on all centroids.
   */
  public abstract long size();

  /**
   * Returns an estimate of a cutoff such that a specified fraction of the data added to this
   * TDigest would be less than or equal to the cutoff.
   *
   * @param q The desired fraction
   * @return The smallest value x such that cdf(x) &ge; q
   */
  public abstract double quantile(double q);

  /**
   * A {@link Collection} that lets you go through the centroids in ascending order by mean.
   * Centroids returned will not be re-used, but may or may not share storage with this TDigest.
   *
   * @return The centroids in the form of a Collection.
   */
  public abstract Collection<Centroid> centroids();

  /**
   * Add a sample to this TDigest.
   *
   * @param x The data value to add
   */
  public abstract void add(double x);

  /**
   * Add all of the centroids of another TDigest to this one.
   *
   * @param other The other TDigest
   */
  public abstract void add(TDigest other);

  public double getMax() {
    return max;
  }

  public void setMax(double max) {
    this.max = max;
  }
}
