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

package com.google.protobuf;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** UncheckedUtf8ByteString. */
public class UncheckedUtf8ByteString extends ByteString.LeafByteString {

  public static ByteString copyFrom(byte[] bytes) {
    return new UncheckedUtf8ByteString(bytes);
  }

  private static final long serialVersionUID = 1L;

  protected final byte[] bytes;

  /**
   * Creates a {@code LiteralByteStringV2} backed by the given array, without copying.
   *
   * @param bytes array to wrap
   */
  UncheckedUtf8ByteString(byte[] bytes) {
    if (bytes == null) {
      throw new NullPointerException();
    }
    this.bytes = bytes;
  }

  @Override
  public byte byteAt(int index) {
    // Unlike most methods in this class, this one is a direct implementation
    // ignoring the potential offset because we need to do range-checking in the
    // substring case anyway.
    return bytes[index];
  }

  @Override
  byte internalByteAt(int index) {
    return bytes[index];
  }

  @Override
  public int size() {
    return bytes.length;
  }

  // =================================================================
  // ByteString -> substring

  @Override
  public final ByteString substring(int beginIndex, int endIndex) {
    checkRange(beginIndex, endIndex, size());
    return new UncheckedUtf8ByteString(Arrays.copyOfRange(bytes, beginIndex, endIndex));
  }

  // =================================================================
  // ByteString -> byte[]

  @Override
  protected void copyToInternal(
      byte[] target, int sourceOffset, int targetOffset, int numberToCopy) {
    // Optimized form, not for subclasses, since we don't call
    // getOffsetIntoBytes() or check the 'numberToCopy' parameter.
    System.arraycopy(bytes, sourceOffset, target, targetOffset, numberToCopy);
  }

  @Override
  public final void copyTo(ByteBuffer target) {
    target.put(bytes, getOffsetIntoBytes(), size()); // Copies bytes
  }

  @Override
  public final ByteBuffer asReadOnlyByteBuffer() {
    return ByteBuffer.wrap(bytes, getOffsetIntoBytes(), size()).asReadOnlyBuffer();
  }

  @Override
  public final List<ByteBuffer> asReadOnlyByteBufferList() {
    return Collections.singletonList(asReadOnlyByteBuffer());
  }

  @Override
  public final void writeTo(OutputStream outputStream) throws IOException {
    outputStream.write(toByteArray());
  }

  @Override
  final void writeToInternal(OutputStream outputStream, int sourceOffset, int numberToWrite)
      throws IOException {
    outputStream.write(bytes, getOffsetIntoBytes() + sourceOffset, numberToWrite);
  }

  @Override
  final void writeTo(ByteOutput output) throws IOException {
    output.writeLazy(bytes, getOffsetIntoBytes(), size());
  }

  @Override
  protected final String toStringInternal(Charset charset) {
    return new String(bytes, getOffsetIntoBytes(), size(), charset);
  }

  // =================================================================
  // UTF-8 decoding

  @Override
  public final boolean isValidUtf8() {
    return true;
  }

  @Override
  protected final int partialIsValidUtf8(int state, int offset, int length) {
    return 0;
  }

  // =================================================================
  // equals() and hashCode()

  @Override
  public final boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof ByteString that)) {
      return false;
    }

    if (size() != that.size()) {
      return false;
    }

    for (int i = 0; i < size(); i++) {
      if (byteAt(i) != that.byteAt(i)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check equality of the substring of given length of this object starting at zero with another
   * {@code LiteralByteStringV2} substring starting at offset.
   *
   * @param other what to compare a substring in
   * @param offset offset into other
   * @param length number of bytes to compare
   * @return true for equality of substrings, else false.
   */
  @Override
  final boolean equalsRange(ByteString other, int offset, int length) {
    if (length > other.size() || offset < 0 || offset + length > other.size()) {
      throw new IllegalArgumentException();
    }

    for (int i = 0; i < length; i++) {
      if (bytes[i] != other.byteAt(offset + i)) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected final int partialHash(int h, int offset, int length) {
    return Internal.partialHash(h, bytes, getOffsetIntoBytes() + offset, length);
  }

  // =================================================================
  // Input stream

  @Override
  public final InputStream newInput() {
    return new ByteArrayInputStream(bytes, getOffsetIntoBytes(), size()); // No copy
  }

  @Override
  public final CodedInputStream newCodedInput() {
    // We trust CodedInputStream not to modify the bytes, or to give anyone
    // else access to them.
    return CodedInputStream.newInstance(
        bytes, getOffsetIntoBytes(), size(), /* bufferIsImmutable= */ true);
  }

  // =================================================================
  // Internal methods

  /**
   * Offset into {@code bytes[]} to use, non-zero for substrings.
   *
   * @return always 0 for this class
   */
  protected int getOffsetIntoBytes() {
    return 0;
  }
}
