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

package name.zicat.astatine.sql.client.test.text;

import static name.zicat.astatine.sql.client.test.text.SqlCommentReaderFilterTest.filter;

import name.zicat.astatine.sql.client.text.FreemarkerReaderFilter;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

/** FreemarkerReaderFilterTest. */
public class FreemarkerReaderFilterTest {

  @Test
  public void testFilter() throws Exception {

    final var filter = new FreemarkerReaderFilter();
    final var s1 =
        """
            <#import "env_utest.ftl" as template>
            <@template.setting tf_idle_state_retention_time='2' />""";
    final var expectedStr =
        """
            SET 'cp.mode'='EXACTLY_ONCE';
            SET cp.alignment.timeout=30000;
            SET cp.interval=60000;
            SET cp.timeout=60000;
            SET cp.force.unaligned=false;
            SET cp.max.concurrent=1;
            SET cp.min.pause.between=2000;
            SET cp.externalized=RETAIN_ON_CANCELLATION;
            SET rs.type=fixed-delay@_,10000;
            SET tf.idle.state.retention.time=2;
            SET tf.pipeline.operator-chaining=true;
            SET tf.table.exec.source.idle-auto-create=0s;""";
    Assert.assertEquals(expectedStr, filter(filter, s1).trim());
  }

  @Test
  public void testHttpSinkDefaults() throws Exception {
    final var sql = renderHttpSink("request\\.type = 'POST'");
    Assert.assertTrue(sql.contains("'connector' = 'http'"));
    Assert.assertTrue(sql.contains("'response.process-mode' = 'default'"));
    Assert.assertTrue(sql.contains("'async.queue.size' = '1024'"));
    Assert.assertFalse(sql.contains("'code.ignore'"));
    Assert.assertFalse(sql.matches("(?s).*,\\s*\\);.*"));
  }

  @Test
  public void testHttpSinkResponseOptions() throws Exception {
    final var ignored =
        renderHttpSink(
            """
            request\\.type = 'POST'
            response\\.process\\-mode\\.default\\.ignore = 'true'
            """);
    Assert.assertTrue(ignored.contains("'response.process-mode.default.ignore' = 'true'"));
    final var specific =
        renderHttpSink(
            """
            request\\.type = 'POST'
            response\\.process\\-mode = 'specific_code'
            response\\.process\\-mode\\.specific_code\\.success\\-codes = '200,204,409'
            """);
    Assert.assertTrue(specific.contains("'response.process-mode' = 'specific_code'"));
    Assert.assertTrue(
        specific.contains("'response.process-mode.specific_code.success-codes' = '200,204,409'"));
    Assert.assertFalse(specific.matches("(?s).*,\\s*\\);.*"));
  }

  @Test
  public void testHttpWechatExample() throws Exception {
    try (var input = getClass().getResourceAsStream("/test_http_wechat_sink.sql")) {
      Assert.assertNotNull(input);
      final var sql =
          filter(
              new FreemarkerReaderFilter(),
              new String(input.readAllBytes(), StandardCharsets.UTF_8));
      Assert.assertTrue(sql.contains("'response.process-mode.default.ignore' = 'true'"));
      Assert.assertTrue(sql.contains("'response.process-mode' = 'default'"));
      Assert.assertTrue(sql.contains("INSERT INTO wechat_sink"));
      Assert.assertFalse(sql.contains("<@"));
    }
  }

  private static String renderHttpSink(String options) throws Exception {
    return filter(
        new FreemarkerReaderFilter(),
        "<#import \"table.ftl\" as template>\n<@template.table_http_sink " + options + " />");
  }
}
