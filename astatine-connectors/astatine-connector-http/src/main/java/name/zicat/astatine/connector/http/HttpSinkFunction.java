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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;

import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/** HttpSinkFunction. */
public class HttpSinkFunction extends RichSinkFunction<RowData> {

  private static final HttpResponse.BodyHandler<Void> DISCARD_BODY_HANDLER =
      HttpResponse.BodyHandlers.discarding();
  private final int[] metadataPositions;
  private final ReadableConfig tableOptions;
  private transient Duration readTimeout;
  private transient RequestType requestType;
  private transient int retryCount;
  private transient Duration retryInterval;
  private transient HttpClient client;
  private transient volatile AtomicReference<Exception> state;
  private transient Function<HttpResponse<Void>, Void> responseHandler;

  public HttpSinkFunction(int[] metadataPositions, ReadableConfig tableOptions) {
    this.metadataPositions = metadataPositions;
    this.tableOptions = tableOptions;
  }

  @Override
  public void open(Configuration parameters) {
    readTimeout = tableOptions.get(READ_TIMEOUT);
    requestType = tableOptions.get(REQUEST_TYPE);
    retryInterval = tableOptions.get(RETRY_INTERVAL);
    retryCount = tableOptions.get(RETRY_COUNT);
    if (retryCount < 0) {
      retryCount = 0;
    }
    final var code400Fail = tableOptions.get(CODE_400_FAIL);
    responseHandler =
        response -> {
          final var code = response.statusCode();
          if (code >= 500) {
            throw new RuntimeException("Target server error, code " + code);
          } else {
            if (code >= 400 && code400Fail) {
              throw new RuntimeException("Bad Request code " + code);
            }
            return response.body();
          }
        };
    state = new AtomicReference<>();
    client = createHttpClient();
  }

  @Override
  public void invoke(RowData row, Context context) throws Exception {
    checkState();
    final String url = metadata(row, HttpWritableMetadata.URL, null);
    if (url == null) {
      return;
    }
    final var headers =
        metadata(row, HttpWritableMetadata.HEADERS, Collections.<String, String>emptyMap());
    final var metaBody = metadata(row, HttpWritableMetadata.BODY, null);
    final var bodyBytes = metaBody == null ? (byte[]) null : (byte[]) metaBody;
    final var builder = HttpRequest.newBuilder().uri(new URI(url));
    headers.forEach(builder::header);
    final var request = requestType.set(builder, bodyBytes).timeout(readTimeout).build();
    client
        .sendAsync(request, DISCARD_BODY_HANDLER)
        .thenApply(responseHandler)
        .exceptionallyAsync(new ExceptionRetryFunction(request, state));
  }

  /**
   * checkState.
   *
   * @throws Exception Exception
   */
  private void checkState() throws Exception {
    final Exception e = state.get();
    if (e != null) {
      state = new AtomicReference<>();
      throw e;
    }
  }

  /** ExceptionRetryFunction. */
  private class ExceptionRetryFunction implements Function<Throwable, Void> {

    private final HttpRequest request;
    private final AtomicReference<Exception> currentState;

    public ExceptionRetryFunction(HttpRequest request, AtomicReference<Exception> currentState) {
      this.request = request;
      this.currentState = currentState;
    }

    @Override
    public Void apply(Throwable throwable) {
      Exception lastestException = throwable instanceof Exception e ? e : new Exception(throwable);
      for (int i = 0; i < retryCount; i++) {
        sleepQuietly();
        try {
          final var response = client.send(request, DISCARD_BODY_HANDLER);
          return responseHandler.apply(response);
        } catch (Exception e) {
          lastestException = e;
        }
      }
      currentState.set(lastestException);
      throw new RuntimeException(lastestException);
    }

    /** sleepQuietly. */
    private void sleepQuietly() {
      try {
        Thread.sleep(retryInterval.toMillis());
      } catch (InterruptedException ignore) {
      }
    }
  }

  /**
   * create http client.
   *
   * @return HttpClient
   */
  private HttpClient createHttpClient() {
    var builder = HttpClient.newBuilder();
    final var proxy = tableOptions.get(PROXY);
    if (proxy != null && !proxy.trim().isEmpty()) {
      final var ipAndPort = proxy.trim().split(":");
      final var address = new InetSocketAddress(ipAndPort[0], Integer.parseInt(ipAndPort[1]));
      builder = builder.proxy(ProxySelector.of(address));
    }
    final var threads = tableOptions.get(ASYNC_THREADS);
    final var queueSize = tableOptions.get(ASYNC_QUEUE_SIZE);
    final var executor =
        new ThreadPoolExecutor(
            threads, threads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(queueSize));
    return builder
        .executor(executor)
        .connectTimeout(tableOptions.get(CONNECT_TIMEOUT))
        .followRedirects(HttpClient.Redirect.ALWAYS)
        .version(HttpClient.Version.HTTP_1_1)
        .build();
  }

  @SuppressWarnings("unchecked")
  private <T> T metadata(RowData consumedRow, HttpWritableMetadata metadata, T defaultValue) {
    final int pos = metadataPositions[metadata.ordinal()];
    if (pos < 0) {
      return defaultValue;
    }
    final var value = metadata.converter.read(consumedRow, pos);
    if (value == null) {
      return defaultValue;
    }
    return (T) value;
  }
}
