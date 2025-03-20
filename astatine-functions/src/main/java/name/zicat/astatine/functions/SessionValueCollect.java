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

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.function.Consumer;

/** SessionValueCollect. */
@FunctionHint(output = @DataTypeHint("ROW<collect_result ARRAY<BIGINT>>"))
public class SessionValueCollect extends TableFunction<Row> {

  public void eval(byte[] valueSeries) throws Exception {
    final var result = new ArrayList<Long>();
    parse(valueSeries, result::add);
    final Row row = new Row(1);
    row.setField(0, result.toArray(new Long[]{}));
    collect(row);
  }

  public static void parse(byte[] valueSeries, Consumer<Long> consumer) throws IOException {
    final var valueIs = new ByteArrayInputStream(valueSeries);
    while (valueIs.available() != 0) {
      final var longValue = readVLong(valueIs);
      consumer.accept(longValue);
    }
  }

  public static Long readVLong(InputStream is) throws IOException {
    long value = 0;
    int shift = 0;
    int aByte;
    while ((aByte = is.read()) != -1) {
      value |= (long) (aByte & 0x7F) << shift;
      if ((aByte & 0x80) == 0) {
        return value;
      }
      shift += 7;
    }
    throw new RuntimeException("read vlong error");
  }
}
