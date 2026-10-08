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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.ValidationException;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;

/** ResponseProcessModeTest. */
public class ResponseProcessModeTest {

  @Test
  public void testDefaultMode() {
    final var config = new Configuration();
    Assert.assertEquals(ResponseProcessMode.DEFAULT, ResponseProcessMode.create(config));
    final var handler =
        ResponseProcessMode.DEFAULT.createResponseHandler(
            runtimeContext(UnregisteredMetricsGroup.createOperatorMetricGroup()), config);
    for (int code : new int[] {200, 204, 301, 399}) {
      Assert.assertNull(handler.apply(response(code)));
    }
    for (int code : new int[] {400, 404, 500, 599}) {
      final var error =
          Assert.assertThrows(RuntimeException.class, () -> handler.apply(response(code)));
      Assert.assertTrue(error.getMessage().contains(String.valueOf(code)));
    }
  }

  @Test
  public void testIgnoredErrorsAreCountedByCode() {
    final var counters = new HashMap<String, Counter>();
    final var group = countingMetricGroup(counters);
    final var config = new Configuration().set(RESPONSE_DEFAULT_IGNORE, true);
    final var handler = ResponseProcessMode.DEFAULT.createResponseHandler(runtimeContext(group), config);
    for (int code : new int[] {200, 399, 400, 400, 503}) {
      Assert.assertNull(handler.apply(response(code)));
    }
    Assert.assertEquals(2, counters.size());
    Assert.assertEquals(2L, counters.get("400").getCount());
    Assert.assertEquals(1L, counters.get("503").getCount());
  }

  @Test
  public void testDefaultIgnoreOption() {
    final var config = new Configuration();
    Assert.assertFalse(config.get(RESPONSE_DEFAULT_IGNORE));
    config.set(RESPONSE_DEFAULT_IGNORE, true);
    Assert.assertTrue(config.get(RESPONSE_DEFAULT_IGNORE));
    config.set(RESPONSE_DEFAULT_IGNORE, false);
    Assert.assertFalse(config.get(RESPONSE_DEFAULT_IGNORE));
  }

  @Test
  public void testSpecificCodeDefaultAndWhitelist() {
    final var config = new Configuration().set(RESPONSE_PROCESS_MODE, "SPECIFIC_CODE");
    final var mode = ResponseProcessMode.create(config);
    Assert.assertEquals(ResponseProcessMode.SPECIFIC_CODE, mode);
    var handler = mode.createResponseHandler(null, config);
    Assert.assertNull(handler.apply(response(200)));
    final var defaultHandler = handler;
    Assert.assertThrows(RuntimeException.class, () -> defaultHandler.apply(response(204)));

    config.set(RESPONSE_SPECIFIC_MODE_SUCCESS_CODES, "200, 204,409, 204");
    // The default-mode ignore flag must not bypass the whitelist.
    config.set(RESPONSE_DEFAULT_IGNORE, true);
    handler = mode.createResponseHandler(null, config);
    for (int code : new int[] {200, 204, 409}) {
      Assert.assertNull(handler.apply(response(code)));
    }
    final var whitelistHandler = handler;
    for (int code : new int[] {201, 301, 400, 500}) {
      Assert.assertThrows(RuntimeException.class, () -> whitelistHandler.apply(response(code)));
    }
  }

  @Test
  public void testInvalidConfiguration() {
    final var config = new Configuration().set(RESPONSE_PROCESS_MODE, "unknown");
    final var error =
        Assert.assertThrows(ValidationException.class, () -> ResponseProcessMode.create(config));
    Assert.assertTrue(error.getMessage().contains(RESPONSE_PROCESS_MODE.key()));
    config.set(RESPONSE_PROCESS_MODE, "specific_code");
    for (String codes : new String[] {"", " ", "abc", "200,", ",200", "200,,204", "99", "600"}) {
      config.set(RESPONSE_SPECIFIC_MODE_SUCCESS_CODES, codes);
      final var planningError =
          Assert.assertThrows(ValidationException.class, () -> ResponseProcessMode.create(config));
      Assert.assertTrue(
          planningError.getMessage().contains(RESPONSE_SPECIFIC_MODE_SUCCESS_CODES.key()));
      final var invalidCodes =
          Assert.assertThrows(
              ValidationException.class,
              () -> ResponseProcessMode.SPECIFIC_CODE.createResponseHandler(null, config));
      Assert.assertTrue(
          invalidCodes.getMessage().contains(RESPONSE_SPECIFIC_MODE_SUCCESS_CODES.key()));
    }
  }

  @Test
  public void testInactiveWhitelistIsNotParsed() {
    final var config = new Configuration().set(RESPONSE_SPECIFIC_MODE_SUCCESS_CODES, "invalid");
    ResponseProcessMode.DEFAULT.createResponseHandler(
        runtimeContext(UnregisteredMetricsGroup.createOperatorMetricGroup()), config);
  }

  @Test
  public void testWhitelistRangeBoundaries() {
    final var config =
        new Configuration()
            .set(RESPONSE_PROCESS_MODE, "specific_code")
            .set(RESPONSE_SPECIFIC_MODE_SUCCESS_CODES, "100,599");
    final var handler = ResponseProcessMode.create(config).createResponseHandler(null, config);
    Assert.assertNull(handler.apply(response(100)));
    Assert.assertNull(handler.apply(response(599)));
    Assert.assertThrows(RuntimeException.class, () -> handler.apply(response(200)));
  }

  private static RuntimeContext runtimeContext(OperatorMetricGroup group) {
    return (RuntimeContext)
        Proxy.newProxyInstance(
            RuntimeContext.class.getClassLoader(),
            new Class<?>[] {RuntimeContext.class},
            (proxy, method, args) -> {
              if (method.getName().equals("getMetricGroup")) {
                return group;
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }

  private static OperatorMetricGroup countingMetricGroup(Map<String, Counter> counters) {
    final MetricGroup group =
        new UnregisteredMetricsGroup() {
          @Override
          public MetricGroup addGroup(String key, String value) {
            Assert.assertEquals("code", key);
            return new UnregisteredMetricsGroup() {
              @Override
              public Counter counter(String name) {
                Assert.assertEquals("http_sink_failed_num", name);
                return counters.computeIfAbsent(value, ignored -> new SimpleCounter());
              }
            };
          }
        };
    return (OperatorMetricGroup)
        Proxy.newProxyInstance(
            OperatorMetricGroup.class.getClassLoader(),
            new Class<?>[] {OperatorMetricGroup.class},
            (proxy, method, args) -> method.invoke(group, args));
  }

  @SuppressWarnings("unchecked")
  private static HttpResponse<Void> response(int statusCode) {
    return (HttpResponse<Void>)
        Proxy.newProxyInstance(
            HttpResponse.class.getClassLoader(),
            new Class<?>[] {HttpResponse.class},
            (proxy, method, args) -> {
              if (method.getName().equals("statusCode")) {
                return statusCode;
              }
              if (method.getName().equals("body")) {
                return null;
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }
}
