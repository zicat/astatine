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

package name.zicat.astatine.connector.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RetryTask.
 */
public interface RetryTask {

    Logger LOG = LoggerFactory.getLogger(RetryTask.class);

    /**
     * run task.
     * @param i retry id, start from 0
     * @throws Exception Exception
     */
    void run(int i) throws Exception;

    /**
     * check whether break, if not break sleep time.
     *
     * @param e e
     * @param i i
     * @param maxRetry maxRetry
     * @return boolean
     */
    default boolean whetherBreak(Exception e, int i, int maxRetry) {
        if (i >= maxRetry) {
            throw new RuntimeException("retry " + maxRetry + " still fail", e);
        }
        LOG.warn("task execute fail on retry {}", i, e);
        return false;
    }
}
