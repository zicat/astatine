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
}
