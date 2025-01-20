/*
 * Copyright 2013 Aggregate Knowledge, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.agkn.hll;

import it.unimi.dsi.fastutil.bytes.AbstractByteCollection;
import it.unimi.dsi.fastutil.bytes.AbstractByteIterator;
import it.unimi.dsi.fastutil.bytes.ByteCollection;
import it.unimi.dsi.fastutil.bytes.ByteIterator;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import java.util.Iterator;
import java.util.Map;

/** CopyAbstractInt2ByteMap. */
public abstract class CopyAbstractInt2ByteMap extends CopyAbstractInt2ByteFunction
    implements Int2ByteMap, java.io.Serializable {

  public boolean containsValue(Object ov) {
    return containsValue(((((Byte) (ov)).byteValue())));
  }

  /** Checks whether the given value is contained in {@link #values()}. */
  public boolean containsValue(byte v) {
    return values().contains(v);
  }

  /** Checks whether the given value is contained in {@link #keySet()}. */
  public boolean containsKey(int k) {
    return keySet().contains(k);
  }

  /**
   * Puts all pairs in the given map. If the map implements the interface of this map, it uses the
   * faster iterators.
   *
   * @param m a map.
   */
  public void putAll(Map<? extends Integer, ? extends Byte> m) {
    int n = m.size();
    final Iterator<? extends Map.Entry<? extends Integer, ? extends Byte>> i =
        m.entrySet().iterator();
    if (m instanceof Int2ByteMap) {
      Entry e;
      while (n-- != 0) {
        e = (Entry) i.next();
        put(e.getIntKey(), e.getByteValue());
      }
    } else {
      Map.Entry<? extends Integer, ? extends Byte> e;
      while (n-- != 0) {
        e = i.next();
        put(e.getKey(), e.getValue());
      }
    }
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  /**
   * This class provides a basic but complete type-specific entry class for all those maps
   * implementations that do not have entries on their own (e.g., most immutable maps).
   *
   * <p>This class does not implement {@link Map.Entry#setValue(Object) setValue()}, as the
   * modification would not be reflected in the base map.
   */
  public static class BasicEntry implements Entry {
    protected int key;
    protected byte value;

    public BasicEntry(final Integer key, final Byte value) {
      this.key = (key);
      this.value = (value);
    }

    public BasicEntry(final int key, final byte value) {
      this.key = key;
      this.value = value;
    }

    public Integer getKey() {
      return (key);
    }

    public int getIntKey() {
      return key;
    }

    public Byte getValue() {
      return (value);
    }

    public byte getByteValue() {
      return value;
    }

    public byte setValue(final byte value) {
      throw new UnsupportedOperationException();
    }

    public Byte setValue(final Byte value) {
      return setValue(value.byteValue());
    }

    public boolean equals(final Object o) {
      if (!(o instanceof Map.Entry<?, ?> e)) {
        return false;
      }
      return ((key) == ((((Integer) (e.getKey()))))) && ((value) == ((((Byte) (e.getValue())))));
    }

    public int hashCode() {
      return (key) ^ (value);
    }

    public String toString() {
      return key + "->" + value;
    }
  }

  /**
   * Returns a type-specific-set view of the keys of this map.
   *
   * <p>The view is backed by the set returned by {@link #entrySet()}. Note that <em>no attempt is
   * made at caching the result of this method</em>, as this would require adding some attributes
   * that lightweight implementations would not need. Subclasses may easily override this policy by
   * calling this method and caching the result, but implementors are encouraged to write more
   * efficient ad-hoc implementations.
   *
   * @return a set view of the keys of this map; it may be safely cast to a type-specific interface.
   */
  public IntSet keySet() {
    return new AbstractIntSet() {
      public boolean contains(final int k) {
        return containsKey(k);
      }

      public int size() {
        return CopyAbstractInt2ByteMap.this.size();
      }

      public void clear() {
        CopyAbstractInt2ByteMap.this.clear();
      }

      public IntIterator iterator() {
        return new AbstractIntIterator() {
          final ObjectIterator<Map.Entry<Integer, Byte>> i = entrySet().iterator();

          public int nextInt() {
            return ((Entry) i.next()).getIntKey();
          }
          ;

          public boolean hasNext() {
            return i.hasNext();
          }
        };
      }
    };
  }

  /**
   * Returns a type-specific-set view of the values of this map.
   *
   * <p>The view is backed by the set returned by {@link #entrySet()}. Note that <em>no attempt is
   * made at caching the result of this method</em>, as this would require adding some attributes
   * that lightweight implementations would not need. Subclasses may easily override this policy by
   * calling this method and caching the result, but implementors are encouraged to write more
   * efficient ad-hoc implementations.
   *
   * @return a set view of the values of this map; it may be safely cast to a type-specific
   *     interface.
   */
  public ByteCollection values() {
    return new AbstractByteCollection() {
      public boolean contains(final byte k) {
        return containsValue(k);
      }

      public int size() {
        return CopyAbstractInt2ByteMap.this.size();
      }

      public void clear() {
        CopyAbstractInt2ByteMap.this.clear();
      }

      public ByteIterator iterator() {
        return new AbstractByteIterator() {
          final ObjectIterator<Map.Entry<Integer, Byte>> i = entrySet().iterator();

          public byte nextByte() {
            return ((Entry) i.next()).getByteValue();
          }
          ;

          public boolean hasNext() {
            return i.hasNext();
          }
        };
      }
    };
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public ObjectSet<Map.Entry<Integer, Byte>> entrySet() {
    return (ObjectSet) int2ByteEntrySet();
  }

  /**
   * Returns a hash code for this map.
   *
   * <p>The hash code of a map is computed by summing the hash codes of its entries.
   *
   * @return a hash code for this map.
   */
  public int hashCode() {
    int h = 0, n = size();
    final ObjectIterator<? extends Map.Entry<Integer, Byte>> i = entrySet().iterator();
    while (n-- != 0) h += i.next().hashCode();
    return h;
  }

  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof Map<?, ?> m)) return false;
    if (m.size() != size()) return false;
    return entrySet().containsAll(m.entrySet());
  }

  public String toString() {
    final StringBuilder s = new StringBuilder();
    final ObjectIterator<? extends Map.Entry<Integer, Byte>> i = entrySet().iterator();
    int n = size();
    Entry e;
    boolean first = true;
    s.append("{");
    while (n-- != 0) {
      if (first) first = false;
      else s.append(", ");
      e = (Entry) i.next();
      s.append(e.getIntKey());
      s.append("=>");
      s.append(String.valueOf(e.getByteValue()));
    }
    s.append("}");
    return s.toString();
  }
}
