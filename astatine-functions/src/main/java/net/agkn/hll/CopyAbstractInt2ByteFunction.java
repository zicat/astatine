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

import it.unimi.dsi.fastutil.ints.Int2ByteFunction;

/** CopyAbstractInt2ByteFunction. */
public abstract class CopyAbstractInt2ByteFunction
    implements Int2ByteFunction, java.io.Serializable {

  /**
   * The default return value for <code>get()</code>, <code>put()</code> and <code>remove()</code> .
   */
  protected byte defRetValue;

  public void defaultReturnValue(final byte rv) {
    defRetValue = rv;
  }

  public byte defaultReturnValue() {
    return defRetValue;
  }

  public byte put(int key, byte value) {
    throw new UnsupportedOperationException();
  }

  public byte remove(int key) {
    throw new UnsupportedOperationException();
  }

  public void clear() {
    throw new UnsupportedOperationException();
  }

  public boolean containsKey(final Object ok) {
    return containsKey(((((Integer) (ok)).intValue())));
  }

  /**
   * Delegates to the corresponding type-specific method, taking care of returning <code>null
   * </code> on a missing key.
   *
   * <p>This method must check whether the provided key is in the map using <code>containsKey()
   * </code>. Thus, it probes the map <em>twice</em>. Implementors of subclasses should override it
   * with a more efficient method.
   */
  public Byte get(final Object ok) {
    final int k = (((Integer) (ok)));
    return containsKey(k) ? (get(k)) : null;
  }

  /**
   * Delegates to the corresponding type-specific method, taking care of returning <code>null
   * </code> on a missing key.
   *
   * <p>This method must check whether the provided key is in the map using <code>containsKey()
   * </code>. Thus, it probes the map <em>twice</em>. Implementors of subclasses should override it
   * with a more efficient method.
   */
  public Byte put(final Integer ok, final Byte ov) {
    final int k = (ok);
    final boolean containsKey = containsKey(k);
    final byte v = put(k, ((ov).byteValue()));
    return containsKey ? (v) : null;
  }

  /**
   * Delegates to the corresponding type-specific method, taking care of returning <code>null
   * </code> on a missing key.
   *
   * <p>This method must check whether the provided key is in the map using <code>containsKey()
   * </code>. Thus, it probes the map <em>twice</em>. Implementors of subclasses should override it
   * with a more efficient method.
   */
  public Byte remove(final Object ok) {
    final int k = (((Integer) (ok)));
    final boolean containsKey = containsKey(k);
    final byte v = remove(k);
    return containsKey ? (v) : null;
  }

  public byte getDefRetValue() {
    return defRetValue;
  }

  public void setDefRetValue(byte defRetValue) {
    this.defRetValue = defRetValue;
  }
}
