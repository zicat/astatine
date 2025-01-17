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

import name.zicat.astatine.sql.client.text.FreemarkerReaderFilter;
import name.zicat.astatine.sql.client.text.SqlCommentReaderFilter;
import name.zicat.astatine.sql.client.text.TextReaderFilter;
import name.zicat.astatine.sql.client.text.TextReaderFilterChain;
import name.zicat.astatine.sql.client.util.StringUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.Reader;
import java.io.StringReader;

/** TextReaderFilterChainText. */
public class TextReaderFilterChainTest {

  @Test
  public void testExecute2() throws Exception {

    final var sql =
        """
                --  /usr/flink/flink-1.17.1/bin/flink-la run-application -p 4 -Dtaskmanager.memory.network.fraction=0.1
                <#import "env_utest.ftl" as template>
                <@template.setting tf_idle_state_retention_time='2' />""";
    final Reader filterReader =
        new TextReaderFilterChain.Builder()
            .addTextReaderFilter(new SqlCommentReaderFilter())
            .addTextReaderFilter(new FreemarkerReaderFilter())
            .build()
            .execute(new StringReader(sql));
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
    Assert.assertEquals(expectedStr, StringUtil.toString(filterReader).trim());
  }

  @Test
  public void testExecute() throws Exception {

    final var filter1 = new TextReaderFilterMock("aaaa");
    final var filter2 = new TextReaderFilterMock("bbbb");
    final var chain =
        new TextReaderFilterChain.Builder().addTextReaderFilter(filter1, filter2).build();
    final var result = chain.execute(new StringReader("cccc"));
    Assert.assertEquals("ccccaaaabbbb", StringUtil.toString(result));

    final var chain2 =
        new TextReaderFilterChain.Builder().addTextReaderFilter(filter2, filter1).build();
    final var result2 = chain2.execute(new StringReader("cccc"));
    Assert.assertEquals("ccccbbbbaaaa", StringUtil.toString(result2));
  }

  /**
   * TextReaderFilterMock.
   * @param append append
   */
  private record TextReaderFilterMock(String append) implements TextReaderFilter {
    @Override
    public Reader filter(Reader reader) throws Exception {
      return new StringReader(StringUtil.toStringBuilder(reader).append(append).toString());
    }
  }
}
