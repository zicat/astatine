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

package name.zicat.astatine.functions;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/** SessionStringValueCollect. */
@FunctionHint(output = @DataTypeHint("ROW<collect_result ARRAY<STRING>>"))
public class SessionStringValueCollect extends TableFunction<Row> {

  public void eval(byte[] valueSeries) throws Exception {
    final var result = new ArrayList<String>();
    final var buffer = ByteBuffer.wrap(valueSeries);
    while (buffer.hasRemaining()) {
      final var size = (int) vLongDecode(buffer);
      final var bytes = new byte[size];
      buffer.get(bytes);
      result.add(new String(bytes, StandardCharsets.UTF_8));
    }
    final Row row = new Row(1);
    row.setField(0, result.toArray(new String[] {}));
    collect(row);
  }

  public static long vLongDecode(ByteBuffer byteBuffer) {
    long value = 0;
    int shift = 0;
    while (byteBuffer.hasRemaining()) {
      final byte b = byteBuffer.get();
      value |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        break;
      }
      shift += 7;
    }
    return value;
  }
}
