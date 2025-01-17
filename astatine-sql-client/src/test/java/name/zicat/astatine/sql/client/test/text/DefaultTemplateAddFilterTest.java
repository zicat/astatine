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

import name.zicat.astatine.sql.client.text.DefaultTemplateAddFilter;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;

/** DefaultTemplateAddFilterTest. */
public class DefaultTemplateAddFilterTest {

  @Test
  public void testFilter() throws Exception {

    final var str1 =
        """
        -- /usr/flink run-application -p 2
        <#import "env_la_offline.ftl" as template>
        <@template.setting/>
        CREATE TABLE AA;
        """;
    final var filter = new DefaultTemplateAddFilter();
    Assert.assertEquals(str1, IOUtils.toString(filter.filter(new StringReader(str1))));

    final var str2 =
        """
        -- /usr/flink run-application -p 2
        <@template.setting/>
        CREATE TABLE AA;
        """;
    final var expectedStr2 =
        """
        <#import "env_local.ftl" as template>
        -- /usr/flink run-application -p 2
        <@template.setting/>
        CREATE TABLE AA;
        """;
    Assert.assertEquals(expectedStr2, IOUtils.toString(filter.filter(new StringReader(str2))));

    final var filter2 = new DefaultTemplateAddFilter("env_aaaa");
    final var expectedStr3 =
        """
        <#import "env_aaaa.ftl" as template>
        -- /usr/flink run-application -p 2
        <@template.setting/>
        CREATE TABLE AA;
        """;
    Assert.assertEquals(expectedStr3, IOUtils.toString(filter2.filter(new StringReader(str2))));
  }
}
