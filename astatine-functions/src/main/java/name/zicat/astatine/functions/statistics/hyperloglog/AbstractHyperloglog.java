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

package name.zicat.astatine.functions.statistics.hyperloglog;

import net.agkn.hll.HLL;
import org.apache.curator.shaded.com.google.common.hash.HashFunction;
import org.apache.curator.shaded.com.google.common.hash.Hasher;
import org.apache.curator.shaded.com.google.common.hash.Hashing;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;

/**
 * AbstractHyperloglog.
 *
 * @param <T>
 */
public abstract class AbstractHyperloglog<T> extends AggregateFunction<T, AbstractHyperloglog.Acc> {

  public static final String NUMBER_OF_SLOT = "hyperloglog.number.slot";
  public static final String NUMBER_OF_PER_SLOT = "hyperloglog.number.per.slot";

  private static final int defaultNumberOfSlot = 4096;
  private static final int defaultNumberOfPerSlot = 8;

  protected int numberOfSlot;
  protected int numberOfPerSlot;

  @Override
  public void open(FunctionContext context) {
    this.numberOfSlot =
        log2m(
            Integer.parseInt(
                context.getJobParameter(NUMBER_OF_SLOT, String.valueOf(defaultNumberOfSlot))));
    if (numberOfPerSlot > 30) {
      numberOfPerSlot = 30;
    } else if (numberOfPerSlot < 4) {
      numberOfPerSlot = 4;
    }
    this.numberOfPerSlot =
        Integer.parseInt(
            context.getJobParameter(NUMBER_OF_PER_SLOT, String.valueOf(defaultNumberOfPerSlot)));
    if (numberOfPerSlot > 8) {
      numberOfPerSlot = 8;
    } else if (numberOfPerSlot < 1) {
      numberOfPerSlot = 1;
    }
  }

  public void accumulate(Acc acc, String value) {
    acc.addValue(value);
  }

  public void accumulate(Acc acc, Integer value) {
    acc.addValue(value);
  }

  public void accumulate(Acc acc, Long value) {
    acc.addValue(value);
  }

  public void accumulate(Acc acc, Short value) {
    acc.addValue(value);
  }

  public void accumulate(Acc acc, Float value) {
    acc.addValue(value);
  }

  public void accumulate(Acc acc, Double value) {
    acc.addValue(value);
  }

  public void accumulate(Acc acc, HLL value) {
    acc.union(value);
  }

  @Override
  public Acc createAccumulator() {
    return new Acc(numberOfSlot, numberOfPerSlot);
  }

  public void merge(Acc acc, Iterable<Acc> it) {
    for (Acc a : it) {
      acc.union(a.hll);
    }
  }

  public static int log2m(int value) {
    int n = 0;
    while (Math.pow(2, n + 1) <= value) {
      n++;
    }
    return n;
  }

  /** Acc. */
  public static class Acc {

    public HLL hll;
    private transient HashFunction hash;

    public Acc(int numberOfSlot, int numberOfPerSlot) {
      this.hll = new HLL(numberOfSlot, numberOfPerSlot);
    }

    public Acc() {}

    public long getValue() {
      return hll.cardinality();
    }

    public void addValue(String value) {
      if (value == null) {
        return;
      }
      try {
        hll.addRaw(
            getOrCreateHasher()
                .putString(value, java.nio.charset.StandardCharsets.UTF_8)
                .hash()
                .asLong());
      } catch (Exception e) {
        throw new RuntimeException("add value " + value + " error", e);
      }
    }

    public void addValue(Integer value) {
      if (value == null) {
        return;
      }
      try {
        hll.addRaw(getOrCreateHasher().putInt(value).hash().asLong());
      } catch (Exception e) {
        throw new RuntimeException("add value " + value + " error", e);
      }
    }

    public void addValue(Long value) {
      if (value == null) {
        return;
      }
      try {
        hll.addRaw(getOrCreateHasher().putLong(value).hash().asLong());
      } catch (Exception e) {
        throw new RuntimeException("add value " + value + " error", e);
      }
    }

    public void addValue(Double value) {
      if (value == null) {
        return;
      }
      try {
        hll.addRaw(getOrCreateHasher().putDouble(value).hash().asLong());
      } catch (Exception e) {
        throw new RuntimeException("add value " + value + " error", e);
      }
    }

    public void addValue(Float value) {
      if (value == null) {
        return;
      }
      try {
        hll.addRaw(getOrCreateHasher().putFloat(value).hash().asLong());
      } catch (Exception e) {
        throw new RuntimeException("add value " + value + " error", e);
      }
    }

    public void addValue(Short value) {
      if (value == null) {
        return;
      }
      try {
        hll.addRaw(getOrCreateHasher().putShort(value).hash().asLong());
      } catch (Exception e) {
        throw new RuntimeException("add value " + value + " error", e);
      }
    }

    public void union(HLL hll) {
      this.hll.union(hll);
    }

    public Hasher getOrCreateHasher() {
      if (hash == null) {
        hash = Hashing.murmur3_128(112233);
      }
      return hash.newHasher();
    }
  }
}
