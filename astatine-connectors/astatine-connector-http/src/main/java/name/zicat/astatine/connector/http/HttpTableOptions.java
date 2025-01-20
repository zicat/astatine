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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.net.http.HttpRequest;
import java.time.Duration;

/** HttpTableOptions. */
public class HttpTableOptions {

  public static final ConfigOption<String> PROXY =
      ConfigOptions.key("proxy")
          .stringType()
          .defaultValue(null)
          .withDescription("the http proxy and port");
  public static final ConfigOption<Duration> CONNECT_TIMEOUT =
      ConfigOptions.key("connect.timeout")
          .durationType()
          .defaultValue(Duration.ofSeconds(10))
          .withDescription("the http connection time out, default 10s");
  public static final ConfigOption<Duration> READ_TIMEOUT =
      ConfigOptions.key("read.timeout")
          .durationType()
          .defaultValue(Duration.ofSeconds(10))
          .withDescription("the http connection time out, default 10s");

  public static final ConfigOption<Duration> RETRY_INTERVAL =
      ConfigOptions.key("retry.interval")
          .durationType()
          .defaultValue(Duration.ofSeconds(1))
          .withDescription("set retry interval");
  public static final ConfigOption<Integer> RETRY_COUNT =
      ConfigOptions.key("retry.count")
          .intType()
          .defaultValue(1)
          .withDeprecatedKeys("set retry count, default12");

  public static final ConfigOption<RequestType> REQUEST_TYPE =
      ConfigOptions.key("request.type")
          .enumType(RequestType.class)
          .noDefaultValue()
          .withDescription("set request type, support value: GET, POST, PUT, DELETE");

  public static final ConfigOption<Integer> ASYNC_QUEUE_SIZE =
      ConfigOptions.key("async.queue.size")
          .intType()
          .defaultValue(1024)
          .withDescription("set async request queue size, default 1024");

  public static final ConfigOption<Integer> ASYNC_THREADS =
      ConfigOptions.key("async.threads")
          .intType()
          .defaultValue(5)
          .withDescription("set async thread count, default 5");

  public static final ConfigOption<Boolean> CODE_400_FAIL =
      ConfigOptions.key("code.400.fail")
          .booleanType()
          .defaultValue(false)
          .withDescription("fail on 400 status code");

  /** RequestType. */
  public enum RequestType {
    GET {
      @Override
      public HttpRequest.Builder set(HttpRequest.Builder builder, byte[] body) {
        return builder.GET();
      }
    },
    POST {
      @Override
      public HttpRequest.Builder set(HttpRequest.Builder builder, byte[] body) {
        return builder.POST(HttpRequest.BodyPublishers.ofByteArray(body));
      }
    },
    PUT {
      @Override
      public HttpRequest.Builder set(HttpRequest.Builder builder, byte[] body) {
        return builder.PUT(HttpRequest.BodyPublishers.ofByteArray(body));
      }
    },
    DELETE {
      @Override
      public HttpRequest.Builder set(HttpRequest.Builder builder, byte[] body) {
        return builder.DELETE();
      }
    };

    RequestType() {}

    /**
     * set request builder.
     *
     * @param builder builder
     * @param body body
     */
    public abstract HttpRequest.Builder set(HttpRequest.Builder builder, byte[] body);
  }
}
