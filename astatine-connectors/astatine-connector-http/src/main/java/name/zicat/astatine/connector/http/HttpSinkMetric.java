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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

/** HttpSinkMetric. */
public class HttpSinkMetric {

    private static final String METRIC_NAME = "http_sink_failed_num";
    private static final String METRIC_KEY_CODE = "code";
    private final MetricGroup metricGroup;
    private final Map<String, Counter> failedCounters = new ConcurrentHashMap<>();

    public HttpSinkMetric(RuntimeContext context) {
        this.metricGroup = context.getMetricGroup();
    }

    public synchronized void writeFailInc(int code) {
        failedCounters
                .computeIfAbsent(
                        String.valueOf(code),
                        c -> metricGroup.addGroup(METRIC_KEY_CODE, c).counter(METRIC_NAME))
                .inc();
    }
}
