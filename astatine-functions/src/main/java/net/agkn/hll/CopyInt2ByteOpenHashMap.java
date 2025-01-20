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

import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.maxFill;

import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.booleans.BooleanArrays;
import it.unimi.dsi.fastutil.bytes.AbstractByteCollection;
import it.unimi.dsi.fastutil.bytes.ByteCollection;
import it.unimi.dsi.fastutil.bytes.ByteIterator;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.AbstractObjectSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Map;
import java.util.NoSuchElementException;

/** CopyCopyInt2ByteOpenHashMap. */
public class CopyInt2ByteOpenHashMap extends CopyAbstractInt2ByteMap
    implements java.io.Serializable, Cloneable, Hash {
  private static boolean ASSERTS = false;

  /** The array of keys. */
  protected int[] key;

  /** The array of values. */
  protected byte[] value;

  /** The array telling whether a position is used. */
  protected boolean[] used;

  /** The acceptable load factor. */
  protected float f;

  /** The current table size. */
  protected int n;

  /** Threshold after which we rehash. It must be the table size times {@link #f}. */
  protected int maxFill;

  /** The mask for wrapping a position counter. */
  protected int mask;

  /** Number of entries in the set. */
  protected int size;

  /**
   * Creates a new hash map.
   *
   * <p>The actual table size will be the least power of two greater than <code>expected</code>/
   * <code>f</code>.
   *
   * @param expected the expected number of elements in the hash set.
   * @param f the load factor.
   */
  public CopyInt2ByteOpenHashMap(final int expected, float f) {
    if (f <= 0 || f > 1)
      throw new IllegalArgumentException(
          "Load factor must be greater than 0 and smaller than or equal to 1");
    if (expected < 0)
      throw new IllegalArgumentException("The expected number of elements must be nonnegative");
    this.f = f;
    n = arraySize(expected, f);
    mask = n - 1;
    maxFill = maxFill(n, f);
    key = new int[n];
    value = new byte[n];
    used = new boolean[n];
  }

  /**
   * Creates a new hash map with {@link Hash#DEFAULT_LOAD_FACTOR} as load factor.
   *
   * @param expected the expected number of elements in the hash map.
   */
  public CopyInt2ByteOpenHashMap(final int expected) {
    this(expected, DEFAULT_LOAD_FACTOR);
  }

  /**
   * Creates a new hash map with initial expected {@link Hash#DEFAULT_INITIAL_SIZE} entries and
   * {@link Hash#DEFAULT_LOAD_FACTOR} as load factor.
   */
  public CopyInt2ByteOpenHashMap() {
    this(DEFAULT_INITIAL_SIZE, DEFAULT_LOAD_FACTOR);
  }

  /**
   * Creates a new hash map copying a given one.
   *
   * @param m a {@link Map} to be copied into the new hash map.
   * @param f the load factor.
   */
  public CopyInt2ByteOpenHashMap(final Map<? extends Integer, ? extends Byte> m, float f) {
    this(m.size(), f);
    putAll(m);
  }

  /**
   * Creates a new hash map with {@link Hash#DEFAULT_LOAD_FACTOR} as load factor copying a given
   * one.
   *
   * @param m a {@link Map} to be copied into the new hash map.
   */
  public CopyInt2ByteOpenHashMap(final Map<? extends Integer, ? extends Byte> m) {
    this(m, DEFAULT_LOAD_FACTOR);
  }

  /**
   * Creates a new hash map copying a given type-specific one.
   *
   * @param m a type-specific map to be copied into the new hash map.
   * @param f the load factor.
   */
  public CopyInt2ByteOpenHashMap(final Int2ByteMap m, float f) {
    this(m.size(), f);
    putAll(m);
  }

  /**
   * Creates a new hash map with {@link Hash#DEFAULT_LOAD_FACTOR} as load factor copying a given
   * type-specific one.
   *
   * @param m a type-specific map to be copied into the new hash map.
   */
  public CopyInt2ByteOpenHashMap(final Int2ByteMap m) {
    this(m, DEFAULT_LOAD_FACTOR);
  }

  /**
   * Creates a new hash map using the elements of two parallel arrays.
   *
   * @param k the array of keys of the new hash map.
   * @param v the array of corresponding values in the new hash map.
   * @param f the load factor.
   * @throws IllegalArgumentException if <code>k</code> and <code>v</code> have different lengths.
   */
  public CopyInt2ByteOpenHashMap(final int[] k, byte[] v, float f) {
    this(k.length, f);
    if (k.length != v.length)
      throw new IllegalArgumentException(
          "The key array and the value array have different lengths ("
              + k.length
              + " and "
              + v.length
              + ")");
    for (int i = 0; i < k.length; i++) this.put(k[i], v[i]);
  }

  /**
   * Creates a new hash map with {@link Hash#DEFAULT_LOAD_FACTOR} as load factor using the elements
   * of two parallel arrays.
   *
   * @param k the array of keys of the new hash map.
   * @param v the array of corresponding values in the new hash map.
   * @throws IllegalArgumentException if <code>k</code> and <code>v</code> have different lengths.
   */
  public CopyInt2ByteOpenHashMap(final int[] k, byte v[]) {
    this(k, v, DEFAULT_LOAD_FACTOR);
  }

  /*
   * The following methods implements some basic building blocks used by
   * all accessors. They are (and should be maintained) identical to those used in OpenHashSet.drv.
   */
  public byte put(final int k, byte v) {
    // The starting point.
    int pos = (HashCommon.murmurHash3((k) ^ mask)) & mask;
    // There's always an unused entry.
    while (used[pos]) {
      if (((key[pos]) == (k))) {
        byte oldValue = value[pos];
        value[pos] = v;
        return oldValue;
      }
      pos = (pos + 1) & mask;
    }
    used[pos] = true;
    key[pos] = k;
    value[pos] = v;
    if (size++ >= maxFill) rehash(arraySize(size + 1, f));
    if (ASSERTS) checkTable();
    return defRetValue;
  }

  public Byte put(final Integer ok, Byte ov) {
    byte v = (ov);
    int k = (ok);
    // The starting point.
    int pos = (HashCommon.murmurHash3((k) ^ mask)) & mask;
    // There's always an unused entry.
    while (used[pos]) {
      if (((key[pos]) == (k))) {
        Byte oldValue = (value[pos]);
        value[pos] = v;
        return oldValue;
      }
      pos = (pos + 1) & mask;
    }
    used[pos] = true;
    key[pos] = k;
    value[pos] = v;
    if (size++ >= maxFill) rehash(arraySize(size + 1, f));
    if (ASSERTS) checkTable();
    return (null);
  }

  /**
   * Adds an increment to value currently associated with a key.
   *
   * @param k the key.
   * @param incr the increment.
   * @return the old value, or the {@linkplain #defaultReturnValue() default return value} if no
   *     value was present for the given key.
   * @deprecated use <code>addTo()</code> instead; having the same name of a {@link java.util.Set}
   *     method turned out to be a recipe for disaster.
   */
  @Deprecated
  public byte add(final int k, byte incr) {
    return addTo(k, incr);
  }

  /**
   * Adds an increment to value currently associated with a key.
   *
   * <p>Note that this method respects the {@linkplain #defaultReturnValue() default return value}
   * semantics: when called with a key that does not currently appears in the map, the key will be
   * associated with the default return value plus the given increment.
   *
   * @param k the key.
   * @param incr the increment.
   * @return the old value, or the {@linkplain #defaultReturnValue() default return value} if no
   *     value was present for the given key.
   */
  public byte addTo(final int k, byte incr) {
    // The starting point.
    int pos = (HashCommon.murmurHash3((k) ^ mask)) & mask;
    // There's always an unused entry.
    while (used[pos]) {
      if (((key[pos]) == (k))) {
        byte oldValue = value[pos];
        value[pos] += incr;
        return oldValue;
      }
      pos = (pos + 1) & mask;
    }
    used[pos] = true;
    key[pos] = k;
    value[pos] = (byte) (defRetValue + incr);
    if (size++ >= maxFill) rehash(arraySize(size + 1, f));
    if (ASSERTS) checkTable();
    return defRetValue;
  }

  /**
   * Shifts left entries with the specified hash code, starting at the specified position, and
   * empties the resulting free entry.
   *
   * @param pos a starting position.
   * @return the position cleared by the shifting process.
   */
  protected int shiftKeys(int pos) {
    // Shift entries with the same hash.
    int last, slot;
    for (; ; ) {
      pos = ((last = pos) + 1) & mask;
      while (used[pos]) {
        slot = (HashCommon.murmurHash3((key[pos]) ^ mask)) & mask;
        if (last <= pos ? last >= slot || slot > pos : last >= slot && slot > pos) break;
        pos = (pos + 1) & mask;
      }
      if (!used[pos]) break;
      key[last] = key[pos];
      value[last] = value[pos];
    }
    used[last] = false;
    return last;
  }

  public byte remove(final int k) {
    // The starting point.
    int pos = (HashCommon.murmurHash3((k) ^ mask)) & mask;
    // There's always an unused entry.
    while (used[pos]) {
      if (((key[pos]) == (k))) {
        size--;
        byte v = value[pos];
        shiftKeys(pos);
        return v;
      }
      pos = (pos + 1) & mask;
    }
    return defRetValue;
  }

  public Byte remove(final Object ok) {
    int k = (((Integer) (ok)));
    // The starting point.
    int pos = (HashCommon.murmurHash3((k) ^ mask)) & mask;
    // There's always an unused entry.
    while (used[pos]) {
      if (((key[pos]) == (k))) {
        size--;
        byte v = value[pos];
        shiftKeys(pos);
        return (v);
      }
      pos = (pos + 1) & mask;
    }
    return (null);
  }

  public Byte get(final Integer ok) {
    int k = (ok);
    // The starting point.
    int pos = (HashCommon.murmurHash3((k) ^ mask)) & mask;
    // There's always an unused entry.
    while (used[pos]) {
      if (((key[pos]) == (k))) return (value[pos]);
      pos = (pos + 1) & mask;
    }
    return (null);
  }

  public byte get(final int k) {
    // The starting point.
    int pos = (HashCommon.murmurHash3((k) ^ mask)) & mask;
    // There's always an unused entry.
    while (used[pos]) {
      if (((key[pos]) == (k))) return value[pos];
      pos = (pos + 1) & mask;
    }
    return defRetValue;
  }

  public boolean containsKey(final int k) {
    // The starting point.
    int pos = (HashCommon.murmurHash3((k) ^ mask)) & mask;
    // There's always an unused entry.
    while (used[pos]) {
      if (((key[pos]) == (k))) return true;
      pos = (pos + 1) & mask;
    }
    return false;
  }

  public boolean containsValue(final byte v) {
    byte[] value = this.value;
    boolean[] used = this.used;
    for (int i = n; i-- != 0; ) if (used[i] && ((value[i]) == (v))) return true;
    return false;
  }

  /* Removes all elements from this map.
   *
   * <P>To increase object reuse, this method does not change the table size.
   * If you want to reduce the table size, you must use {@link #trim()}.
   *
   */
  public void clear() {
    if (size == 0) return;
    size = 0;
    BooleanArrays.fill(used, false);
    // We null all object entries so that the garbage collector can do its work.
  }

  public int size() {
    return size;
  }

  public boolean isEmpty() {
    return size == 0;
  }

  /**
   * A no-op for backward compatibility.
   *
   * @param growthFactor unused.
   * @deprecated Since <code>fastutil</code> 6.1.0, hash tables are doubled when they are too full.
   */
  @Deprecated
  public void growthFactor(int growthFactor) {}

  /**
   * Gets the growth factor (2).
   *
   * @return the growth factor of this set, which is fixed (2).
   * @see #growthFactor(int)
   * @deprecated Since <code>fastutil</code> 6.1.0, hash tables are doubled when they are too full.
   */
  @Deprecated
  public int growthFactor() {
    return 16;
  }

  /**
   * The entry class for a hash map does not record key and value, but rather the position in the
   * hash table of the corresponding entry. This is necessary so that calls to {@link
   * Map.Entry#setValue(Object)} are reflected in the map
   */
  private class MapEntry implements Int2ByteMap.Entry, Map.Entry<Integer, Byte> {
    // The table index this entry refers to, or -1 if this entry has been deleted.
    private int index;

    MapEntry(final int index) {
      this.index = index;
    }

    public Integer getKey() {
      return (key[index]);
    }

    public int getIntKey() {
      return key[index];
    }

    public Byte getValue() {
      return (value[index]);
    }

    public byte getByteValue() {
      return value[index];
    }

    public byte setValue(final byte v) {
      byte oldValue = value[index];
      value[index] = v;
      return oldValue;
    }

    public Byte setValue(final Byte v) {
      return (setValue(((v).byteValue())));
    }

    @SuppressWarnings("unchecked")
    public boolean equals(final Object o) {
      if (!(o instanceof Map.Entry)) return false;
      Map.Entry<Integer, Byte> e = (Map.Entry<Integer, Byte>) o;
      return ((key[index]) == ((e.getKey()))) && ((value[index]) == ((e.getValue())));
    }

    public int hashCode() {
      return (key[index]) ^ (value[index]);
    }

    public String toString() {
      return key[index] + "=>" + value[index];
    }
  }

  /** An iterator over a hash map. */
  private class MapIterator {
    /**
     * The index of the next entry to be returned, if positive or zero. If negative, the next entry
     * to be returned, if any, is that of index -pos -2 from the {@link #wrapped} list.
     */
    int pos = CopyInt2ByteOpenHashMap.this.n;

    /**
     * The index of the last entry that has been returned. It is -1 if either we did not return an
     * entry yet, or the last returned entry has been removed.
     */
    int last = -1;

    /** A downward counter measuring how many entries must still be returned. */
    int c = size;

    /**
     * A lazily allocated list containing the keys of elements that have wrapped around the table
     * because of removals; such elements would not be enumerated (other elements would be usually
     * enumerated twice in their place).
     */
    IntArrayList wrapped;

    {
      boolean[] used = CopyInt2ByteOpenHashMap.this.used;
      if (c != 0)
        while (!used[--pos])
          ;
    }

    public boolean hasNext() {
      return c != 0;
    }

    public int nextEntry() {
      if (!hasNext()) throw new NoSuchElementException();
      c--;
      // We are just enumerating elements from the wrapped list.
      if (pos < 0) {
        int k = wrapped.getInt(-(last = --pos) - 2);
        // The starting point.
        int pos = (HashCommon.murmurHash3((k) ^ mask)) & mask;
        // There's always an unused entry.
        while (used[pos]) {
          if (((key[pos]) == (k))) return pos;
          pos = (pos + 1) & mask;
        }
      }
      last = pos;
      // System.err.println( "Count: " + c );
      if (c != 0) {
        boolean[] used = CopyInt2ByteOpenHashMap.this.used;
        while (pos-- != 0 && !used[pos])
          ;
        // When here pos < 0 there are no more elements to be enumerated by scanning, but
        // wrapped might be nonempty.
      }
      return last;
    }

    /**
     * Shifts left entries with the specified hash code, starting at the specified position, and
     * empties the resulting free entry. If any entry wraps around the table, instantiates lazily
     * {@link #wrapped} and stores the entry key.
     *
     * @param pos a starting position.
     * @return the position cleared by the shifting process.
     */
    protected int shiftKeys(int pos) {
      // Shift entries with the same hash.
      int last, slot;
      for (; ; ) {
        pos = ((last = pos) + 1) & mask;
        while (used[pos]) {
          slot = (HashCommon.murmurHash3((key[pos]) ^ mask)) & mask;
          if (last <= pos ? last >= slot || slot > pos : last >= slot && slot > pos) break;
          pos = (pos + 1) & mask;
        }
        if (!used[pos]) break;
        if (pos < last) {
          // Wrapped entry.
          if (wrapped == null) wrapped = new IntArrayList();
          wrapped.add(key[pos]);
        }
        key[last] = key[pos];
        value[last] = value[pos];
      }
      used[last] = false;
      return last;
    }

    public void remove() {
      if (last == -1) throw new IllegalStateException();
      if (pos < -1) {
        // We're removing wrapped entries.
        CopyInt2ByteOpenHashMap.this.remove(wrapped.getInt(-pos - 2));
        last = -1;
        return;
      }
      size--;
      if (shiftKeys(last) == pos && c > 0) {
        c++;
        nextEntry();
      }
      last = -1; // You can no longer remove this entry.
      if (ASSERTS) checkTable();
    }

    public int skip(final int n) {
      int i = n;
      while (i-- != 0 && hasNext()) nextEntry();
      return n - i - 1;
    }
  }

  private class EntryIterator extends MapIterator implements ObjectIterator<Entry> {
    private MapEntry entry;

    public Int2ByteMap.Entry next() {
      return entry = new MapEntry(nextEntry());
    }

    @Override
    public void remove() {
      super.remove();
      entry.index = -1; // You cannot use a deleted entry.
    }
  }

  private class FastEntryIterator extends MapIterator implements ObjectIterator<Int2ByteMap.Entry> {
    BasicEntry entry = new BasicEntry(0, ((byte) 0));

    public BasicEntry next() {
      int e = nextEntry();
      entry.key = key[e];
      entry.value = value[e];
      return entry;
    }
  }

  public class MapEntrySet extends AbstractObjectSet<Entry> implements FastEntrySet {
    public ObjectIterator<Int2ByteMap.Entry> iterator() {
      return new EntryIterator();
    }

    public ObjectIterator<Int2ByteMap.Entry> fastIterator() {
      return new FastEntryIterator();
    }

    @SuppressWarnings("unchecked")
    public boolean contains(final Object o) {
      if (!(o instanceof Map.Entry)) return false;
      Map.Entry<Integer, Byte> e = (Map.Entry<Integer, Byte>) o;
      int k = (e.getKey());
      // The starting point.
      int pos = (HashCommon.murmurHash3((k) ^ mask)) & mask;
      // There's always an unused entry.
      while (used[pos]) {
        if (((key[pos]) == (k))) return ((value[pos]) == ((e.getValue())));
        pos = (pos + 1) & mask;
      }
      return false;
    }

    @SuppressWarnings("unchecked")
    public boolean remove(final Object o) {
      if (!(o instanceof Map.Entry)) return false;
      Map.Entry<Integer, Byte> e = (Map.Entry<Integer, Byte>) o;
      int k = (e.getKey());
      // The starting point.
      int pos = (HashCommon.murmurHash3((k) ^ mask)) & mask;
      // There's always an unused entry.
      while (used[pos]) {
        if (((key[pos]) == (k))) {
          CopyInt2ByteOpenHashMap.this.remove(e.getKey());
          return true;
        }
        pos = (pos + 1) & mask;
      }
      return false;
    }

    public int size() {
      return size;
    }

    public void clear() {
      CopyInt2ByteOpenHashMap.this.clear();
    }
  }

  public FastEntrySet int2ByteEntrySet() {
    return new MapEntrySet();
  }

  /**
   * An iterator on keys.
   *
   * <p>We simply override the {@link java.util.ListIterator#next()}/{@link
   * java.util.ListIterator#previous()} methods (and possibly their type-specific counterparts) so
   * that they return keys instead of entries.
   */
  private class KeyIterator extends MapIterator implements IntIterator {
    public KeyIterator() {
      super();
    }

    public int nextInt() {
      return key[nextEntry()];
    }

    public Integer next() {
      return (key[nextEntry()]);
    }
  }

  private class KeySet extends AbstractIntSet {
    public IntIterator iterator() {
      return new KeyIterator();
    }

    public int size() {
      return size;
    }

    public boolean contains(int k) {
      return containsKey(k);
    }

    public boolean remove(int k) {
      int oldSize = size;
      CopyInt2ByteOpenHashMap.this.remove(k);
      return size != oldSize;
    }

    public void clear() {
      CopyInt2ByteOpenHashMap.this.clear();
    }
  }

  public IntSet keySet() {
    return new KeySet();
  }

  /**
   * An iterator on values.
   *
   * <p>We simply override the {@link java.util.ListIterator#next()}/{@link
   * java.util.ListIterator#previous()} methods (and possibly their type-specific counterparts) so
   * that they return values instead of entries.
   */
  private class ValueIterator extends MapIterator implements ByteIterator {
    public ValueIterator() {
      super();
    }

    public byte nextByte() {
      return value[nextEntry()];
    }

    public Byte next() {
      return (value[nextEntry()]);
    }
  }

  public ByteCollection values() {
    return new AbstractByteCollection() {
      public ByteIterator iterator() {
        return new ValueIterator();
      }

      public int size() {
        return size;
      }

      public boolean contains(byte v) {
        return containsValue(v);
      }

      public void clear() {
        CopyInt2ByteOpenHashMap.this.clear();
      }
    };
  }

  /**
   * A no-op for backward compatibility. The kind of tables implemented by this class never need
   * rehashing.
   *
   * <p>If you need to reduce the table size to fit exactly this set, use {@link #trim()}.
   *
   * @return true.
   * @see #trim()
   * @deprecated A no-op.
   */
  @Deprecated
  public boolean rehash() {
    return true;
  }

  /**
   * Rehashes the map, making the table as small as possible.
   *
   * <p>This method rehashes the table to the smallest size satisfying the load factor. It can be
   * used when the set will not be changed anymore, so to optimize access speed and size.
   *
   * <p>If the table size is already the minimum possible, this method does nothing.
   *
   * @return true if there was enough memory to trim the map.
   * @see #trim(int)
   */
  public boolean trim() {
    int l = arraySize(size, f);
    if (l >= n) return true;
    try {
      rehash(l);
    } catch (OutOfMemoryError cantDoIt) {
      return false;
    }
    return true;
  }

  /**
   * Rehashes this map if the table is too large.
   *
   * <p>Let <var>N</var> be the smallest table size that can hold <code>max(n,{@link #size()})
   * </code> entries, still satisfying the load factor. If the current table size is smaller than or
   * equal to <var>N</var>, this method does nothing. Otherwise, it rehashes this map in a table of
   * size <var>N</var>.
   *
   * <p>This method is useful when reusing maps. {@linkplain #clear() Clearing a map} leaves the
   * table size untouched. If you are reusing a map many times, you can call this method with a
   * typical size to avoid keeping around a very large table just because of a few large maps.
   *
   * @param n the threshold for the trimming.
   * @return true if there was enough memory to trim the map.
   * @see #trim()
   */
  public boolean trim(final int n) {
    int l = HashCommon.nextPowerOfTwo((int) Math.ceil(n / f));
    if (this.n <= l) return true;
    try {
      rehash(l);
    } catch (OutOfMemoryError cantDoIt) {
      return false;
    }
    return true;
  }

  /**
   * Rehashes the map.
   *
   * <p>This method implements the basic rehashing strategy, and may be overriden by subclasses
   * implementing different rehashing strategies (e.g., disk-based rehashing). However, you should
   * not override this method unless you understand the internal workings of this class.
   *
   * @param newN the new size
   */
  protected void rehash(final int newN) {
    int i = 0, pos;
    boolean[] used = this.used;
    int k;
    int[] key = this.key;
    byte[] value = this.value;
    int mask = newN - 1; // Note that this is used by the hashing macro
    int[] newKey = new int[newN];
    byte[] newValue = new byte[newN];
    boolean[] newUsed = new boolean[newN];
    for (int j = size; j-- != 0; ) {
      while (!used[i]) i++;
      k = key[i];
      pos = (HashCommon.murmurHash3((k) ^ mask)) & mask;
      while (newUsed[pos]) pos = (pos + 1) & mask;
      newUsed[pos] = true;
      newKey[pos] = k;
      newValue[pos] = value[i];
      i++;
    }
    n = newN;
    this.mask = mask;
    maxFill = maxFill(n, f);
    this.key = newKey;
    this.value = newValue;
    this.used = newUsed;
  }

  /**
   * Returns a deep copy of this map.
   *
   * <p>This method performs a deep copy of this hash map; the data stored in the map, however, is
   * not cloned. Note that this makes a difference only for object keys.
   *
   * @return a deep copy of this map.
   */
  public CopyInt2ByteOpenHashMap clone() {
    CopyInt2ByteOpenHashMap c;
    try {
      c = (CopyInt2ByteOpenHashMap) super.clone();
    } catch (CloneNotSupportedException cantHappen) {
      throw new InternalError();
    }
    c.key = key.clone();
    c.value = value.clone();
    c.used = used.clone();
    return c;
  }

  /**
   * Returns a hash code for this map.
   *
   * <p>This method overrides the generic method provided by the superclass. Since <code>equals()
   * </code> is not overriden, it is important that the value returned by this method is the same
   * value as the one returned by the overriden method.
   *
   * @return a hash code for this map.
   */
  public int hashCode() {
    int h = 0;
    for (int j = size, i = 0, t = 0; j-- != 0; ) {
      while (!used[i]) i++;
      t = (key[i]);
      t ^= (value[i]);
      h += t;
      i++;
    }
    return h;
  }

  private void writeObject(java.io.ObjectOutputStream s) throws java.io.IOException {
    int[] key = this.key;
    byte[] value = this.value;
    MapIterator i = new MapIterator();
    s.defaultWriteObject();
    for (int j = size, e; j-- != 0; ) {
      e = i.nextEntry();
      s.writeInt(key[e]);
      s.writeByte(value[e]);
    }
  }

  private void readObject(java.io.ObjectInputStream s)
      throws java.io.IOException, ClassNotFoundException {
    s.defaultReadObject();
    n = arraySize(size, f);
    maxFill = maxFill(n, f);
    mask = n - 1;
    int[] key = this.key = new int[n];
    byte[] value = this.value = new byte[n];
    boolean[] used = this.used = new boolean[n];
    int k;
    byte v;
    for (int i = size, pos = 0; i-- != 0; ) {
      k = s.readInt();
      v = s.readByte();
      pos = (HashCommon.murmurHash3((k) ^ mask)) & mask;
      while (used[pos]) pos = (pos + 1) & mask;
      used[pos] = true;
      key[pos] = k;
      value[pos] = v;
    }
    if (ASSERTS) checkTable();
  }

  private void checkTable() {}

  public static class BasicEntry implements Int2ByteMap.Entry {
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
      if (!(o instanceof Map.Entry<?, ?> e)) return false;
      return ((key) == ((((Integer) (e.getKey()))))) && ((value) == ((((Byte) (e.getValue())))));
    }

    public int hashCode() {
      return (key) ^ (value);
    }

    public String toString() {
      return key + "->" + value;
    }

    public BasicEntry() {}

    public void setKey(int key) {
      this.key = key;
    }
  }

  public CopyInt2ByteOpenHashMap(
      int[] key, byte[] value, boolean[] used, float f, int n, int maxFill, int mask, int size) {
    this.key = key;
    this.value = value;
    this.used = used;
    this.f = f;
    this.n = n;
    this.maxFill = maxFill;
    this.mask = mask;
    this.size = size;
  }

  public static boolean isASSERTS() {
    return ASSERTS;
  }

  public static void setASSERTS(boolean ASSERTS) {
    CopyInt2ByteOpenHashMap.ASSERTS = ASSERTS;
  }

  public int[] getKey() {
    return key;
  }

  public void setKey(int[] key) {
    this.key = key;
  }

  public byte[] getValue() {
    return value;
  }

  public void setValue(byte[] value) {
    this.value = value;
  }

  public boolean[] getUsed() {
    return used;
  }

  public void setUsed(boolean[] used) {
    this.used = used;
  }

  public float getF() {
    return f;
  }

  public void setF(float f) {
    this.f = f;
  }

  public int getN() {
    return n;
  }

  public void setN(int n) {
    this.n = n;
  }

  public int getMaxFill() {
    return maxFill;
  }

  public void setMaxFill(int maxFill) {
    this.maxFill = maxFill;
  }

  public int getMask() {
    return mask;
  }

  public void setMask(int mask) {
    this.mask = mask;
  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }
}
