// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package name.zicat.astatine.connector.doris.table;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import name.zicat.astatine.connector.doris.util.StreamLoadException;
import name.zicat.astatine.connector.doris.model.RespContent;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.NoConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

/** DorisStreamLoad. */
public class DorisStreamLoad implements Closeable {

  private static final List<String> DORIS_SUCCESS_STATUS =
      new ArrayList<>(Arrays.asList("Success", "Label Already Exists"));
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final CloseableHttpClient httpClient;

  public DorisStreamLoad(long connectionTimeout, long socketTimeout, int threads) {
    this.httpClient = createHttpClient(connectionTimeout, socketTimeout, threads);
  }

  public RespContent streamLoad(String url, byte[] values, Map<String, String> headers)
      throws IOException, StreamLoadException {
    final var put = new HttpPut(url);
    for (var entry : headers.entrySet()) {
      put.setHeader(entry.getKey(), entry.getValue());
    }
    put.setEntity(textPlainEntity(values));
    try (final var response = httpClient.execute(put)) {
      final var statusCode = response.getStatusLine().getStatusCode();
      final var reasonPhrase = response.getStatusLine().getReasonPhrase();
      if (statusCode != 200 || response.getEntity() == null) {
        throw new StreamLoadException("stream load error: " + reasonPhrase);
      }
      final var respContent =
          OBJECT_MAPPER.readValue(EntityUtils.toString(response.getEntity()), RespContent.class);
      if (!DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
        throw new StreamLoadException(errMsg(respContent));
      }
      return respContent;
    }
  }

  /**
   * text plain entity.
   *
   * @param values values
   * @return HttpEntity
   */
  private static HttpEntity textPlainEntity(byte[] values) {
    return new ByteArrayEntity(
        values, ContentType.create(ContentType.TEXT_PLAIN.getMimeType(), StandardCharsets.UTF_8));
  }

  /**
   * error message.
   *
   * @param respContent respContent
   * @return error message
   */
  private static String errMsg(RespContent respContent) {
    return String.format(
        "stream load error: %s, see more in %s",
        respContent.getMessage(), respContent.getErrorURL());
  }

  /**
   * create http client by options.
   *
   * @param connectionTimeout connectionTimeout
   * @param readTimeout readTimeout
   * @return CloseableHttpClient
   */
  private static CloseableHttpClient createHttpClient(
      long connectionTimeout, long readTimeout, int maxConnPerRoute) {
    return HttpClients.custom()
        .setRedirectStrategy(
            new DefaultRedirectStrategy() {
              @Override
              protected boolean isRedirectable(String method) {
                return true;
              }
            })
        .setDefaultRequestConfig(
            RequestConfig.custom()
                .setConnectTimeout((int) connectionTimeout)
                .setSocketTimeout((int) readTimeout)
                .build())
        .setConnectionReuseStrategy(NoConnectionReuseStrategy.INSTANCE)
        .setMaxConnPerRoute(maxConnPerRoute)
        .setMaxConnTotal(maxConnPerRoute)
        .build();
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(httpClient);
  }
}
