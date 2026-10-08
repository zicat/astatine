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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import java.net.http.HttpResponse;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import static name.zicat.astatine.connector.http.HttpTableOptions.*;

/** ResponseProcessMode. */
public enum ResponseProcessMode {
  DEFAULT("default") {
    @Override
    public Function<HttpResponse<Void>, Void> createResponseHandler(
        RuntimeContext context, ReadableConfig config) {
      final boolean codeIgnore = config.get(RESPONSE_DEFAULT_IGNORE);
      final var metric = new HttpSinkMetric(context);
      return response -> {
        final var code = response.statusCode();
        if (codeIgnore) {
          if (code >= 400) {
            metric.writeFailInc(code);
          }
          return response.body();
        }
        if (code >= 400) {
          throw new RuntimeException("Unexpected code " + code);
        }
        return response.body();
      };
    }
  },

  SPECIFIC_CODE("specific_code") {
    @Override
    public Function<HttpResponse<Void>, Void> createResponseHandler(
        RuntimeContext context, ReadableConfig config) {
      final var successCodeSet = parseSuccessCodes(config);
      return response -> {
        final var code = response.statusCode();
        if (successCodeSet.contains(code)) {
          return response.body();
        }
        throw new RuntimeException("Unexpected code " + code);
      };
    }
  };

  private final String type;

  ResponseProcessMode(String type) {
    this.type = type;
  }

  public final String type() {
    return type;
  }

  /**
   * create response handler.
   *
   * @param context context
   * @param config config
   * @return handler
   */
  public abstract Function<HttpResponse<Void>, Void> createResponseHandler(
      RuntimeContext context, ReadableConfig config);

  public static ResponseProcessMode create(ReadableConfig config) {
    final String value = config.get(RESPONSE_PROCESS_MODE);
    for (var type : ResponseProcessMode.values()) {
      if (type.type.equalsIgnoreCase(value)) {
        if (type == SPECIFIC_CODE) {
          parseSuccessCodes(config);
        }
        return type;
      }
    }
    throw new ValidationException(
        "Invalid '"
            + RESPONSE_PROCESS_MODE.key()
            + "': "
            + value
            + ". Supported values are 'default' and 'specific_code'.");
  }

  private static Set<Integer> parseSuccessCodes(ReadableConfig config) {
    final String value = config.get(RESPONSE_SPECIFIC_MODE_SUCCESS_CODES);
    final Set<Integer> codes = new HashSet<>();
    try {
      for (String token : value.split(",", -1)) {
        final int code = Integer.parseInt(token.trim());
        if (code < 100 || code > 599) {
          throw new NumberFormatException("Status code out of range: " + code);
        }
        codes.add(code);
      }
    } catch (NumberFormatException e) {
      throw new ValidationException(
          "Invalid '"
              + RESPONSE_SPECIFIC_MODE_SUCCESS_CODES.key()
              + "': "
              + value
              + ". Expected a non-empty comma-separated list of HTTP status codes (100-599).",
          e);
    }
    return codes;
  }
}
