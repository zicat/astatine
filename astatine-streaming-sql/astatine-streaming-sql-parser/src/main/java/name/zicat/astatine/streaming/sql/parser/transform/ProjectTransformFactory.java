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

package name.zicat.astatine.streaming.sql.parser.transform;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStream;

/** ProjectTransformFactory. */
public class ProjectTransformFactory extends OneTransformFactory {

  public static final String IDENTITY = "PROJECT";
  public static final ConfigOption<String> OPTION_PROJECT_FIELD_INDEXES =
      ConfigOptions.key("field_indexes").stringType().defaultValue(null);

  @Override
  public DataStream<?> transform(TransformContext context, DataStream<?> stream) {
    final var fieldIndexesValue = context.get(OPTION_PROJECT_FIELD_INDEXES);
    if (fieldIndexesValue == null) {
      throw new RuntimeException(
          "project operator must config " + OPTION_PROJECT_FIELD_INDEXES.key());
    }
    return stream.project(fieldIndexes(fieldIndexesValue));
  }

  /**
   * parse field indexes value.
   *
   * @param fieldIndexesValue fieldIndexesValue
   * @return index array
   */
  private static int[] fieldIndexes(String fieldIndexesValue) {
    final var split = fieldIndexesValue.split(",");
    final int[] result = new int[split.length];
    for (int i = 0; i < split.length; i++) {
      result[i] = Integer.parseInt(split[i].trim());
    }
    return result;
  }

  @Override
  public String identity() {
    return IDENTITY;
  }
}
