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

package name.zicat.astatine.sql.client.util;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Objects;

/** FileUtils. */
public class FileUtil {

  /**
   * create file reader.
   *
   * @param fileName fileName
   * @param charset charset
   * @return reader
   * @throws FileNotFoundException FileNotFoundException
   */
  public static Reader createReader(String fileName, Charset charset) throws FileNotFoundException {
    var file = new File(fileName);
    if (!file.exists() || !file.isFile()) {
      final var url =
          Objects.requireNonNull(
              Thread.currentThread().getContextClassLoader().getResource(fileName));
      file = new File(url.getFile());
    }
    return new InputStreamReader(new FileInputStream(file), charset);
  }
}
