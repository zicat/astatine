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

package name.zicat.astatine.connector.http;

import static name.zicat.astatine.connector.http.HttpTableOptions.*;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** HttpDynamicTableFactoryTest. */
public class HttpDynamicTableFactoryTest {

  @Test
  public void testValidOptions() {
    final var config = options();
    Assert.assertFalse(createSink(config).tableOptions.get(RESPONSE_DEFAULT_IGNORE));
    config.put(RESPONSE_DEFAULT_IGNORE.key(), "true");
    Assert.assertTrue(createSink(config).tableOptions.get(RESPONSE_DEFAULT_IGNORE));
    config.put(RESPONSE_DEFAULT_IGNORE.key(), "false");
    Assert.assertFalse(createSink(config).tableOptions.get(RESPONSE_DEFAULT_IGNORE));
    config.put(RESPONSE_PROCESS_MODE.key(), "specific_code");
    config.put(RESPONSE_SPECIFIC_MODE_SUCCESS_CODES.key(), "200, 204,409");
    Assert.assertNotNull(createSink(config));
  }

  @Test
  public void testLegacyIgnoreOptionIsRejected() {
    final var config = options();
    config.put("code.ignore", "true");
    final var error =
        Assert.assertThrows(ValidationException.class, () -> createSink(config));
    Assert.assertTrue(error.getMessage().contains("code.ignore"));
  }

  @Test
  public void testInvalidModeFailsDuringPlanning() {
    final var config = options();
    config.put(RESPONSE_PROCESS_MODE.key(), "specific-code");
    final var error =
        Assert.assertThrows(ValidationException.class, () -> createSink(config));
    Assert.assertTrue(error.getMessage().contains(RESPONSE_PROCESS_MODE.key()));
  }

  @Test
  public void testInvalidWhitelistFailsDuringPlanning() {
    final var config = options();
    config.put(RESPONSE_PROCESS_MODE.key(), "specific_code");
    for (String codes : new String[] {"", "200,", "invalid", "600"}) {
      config.put(RESPONSE_SPECIFIC_MODE_SUCCESS_CODES.key(), codes);
      final var error =
          Assert.assertThrows(ValidationException.class, () -> createSink(config));
      Assert.assertTrue(error.getMessage().contains(RESPONSE_SPECIFIC_MODE_SUCCESS_CODES.key()));
    }
  }

  @Test
  public void testUnknownOptionsAreRejected() {
    final var config = options();
    config.put("response.process-mode.default.ignroe", "true");
    Assert.assertThrows(ValidationException.class, () -> createSink(config));
  }

  @Test
  public void testResponseOptionNames() {
    final var config = options();
    config.put("response.process-mode", "specific_code");
    config.put("response.process-mode.default.ignore", "true");
    config.put("response.process-mode.specific_code.success-codes", "200,204,409");
    final var sink = createSink(config);
    Assert.assertEquals(
        ResponseProcessMode.SPECIFIC_CODE, ResponseProcessMode.create(sink.tableOptions));
    Assert.assertTrue(sink.tableOptions.get(RESPONSE_DEFAULT_IGNORE));
    Assert.assertEquals("200,204,409", sink.tableOptions.get(RESPONSE_SPECIFIC_MODE_SUCCESS_CODES));
  }

  @Test
  public void testInactiveWhitelistIsNotValidatedDuringPlanning() {
    final var config = options();
    config.put("response.process-mode", "default");
    config.put("response.process-mode.specific_code.success-codes", "invalid");
    Assert.assertNotNull(createSink(config));
  }

  private static Map<String, String> options() {
    final var options = new HashMap<String, String>();
    options.put("connector", "http");
    options.put(REQUEST_TYPE.key(), "POST");
    return options;
  }

  private static HttpDynamicSink createSink(Map<String, String> options) {
    final var schema = Schema.newBuilder().column("body", DataTypes.BYTES()).build();
    final var table =
        new ResolvedCatalogTable(
            CatalogTable.of(schema, "", List.of(), options),
            ResolvedSchema.of(Column.physical("body", DataTypes.BYTES())));
    final var context =
        new FactoryUtil.DefaultDynamicTableContext(
            ObjectIdentifier.of("catalog", "database", "http_sink"),
            table,
            Map.of(),
            new Configuration(),
            HttpDynamicTableFactoryTest.class.getClassLoader(),
            false);
    return (HttpDynamicSink) new HttpDynamicTableFactory().createDynamicTableSink(context);
  }
}
