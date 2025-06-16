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

package name.zicat.astatine.connector.doris.table;

import java.io.Closeable;
import java.sql.SQLException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static name.zicat.astatine.connector.doris.util.DorisJDBCUtils.getBackendsV2;
import static name.zicat.astatine.connector.base.ListUtils.diff;

/** DorisHealthChecker. */
public class DorisHealthChecker implements Closeable {

  private static final int DEFAULT_REFRESH_MILLS = 30000;
  private static final Logger LOG = LoggerFactory.getLogger(DorisHealthChecker.class);
  private final Random random = new Random();
  private final String fenodes;
  private final String username;
  private final String password;
  private final String db;
  private final String tbl;
  private final ScheduledExecutorService beRefreshExecutorService;
  private long preConnectionTs = System.currentTimeMillis();
  private volatile List<String> backends;

  public DorisHealthChecker(
      String fenodes, String username, String password, String db, String tbl) {
    this.fenodes = fenodes;
    this.username = username;
    this.password = password;
    this.db = db;
    this.tbl = tbl;
    this.backends = sortedActiveBackends(fenodes, username, password);
    LOG.info("active be list {}", backends);
    this.beRefreshExecutorService = createAndInitRefreshService();
  }

  public String random() {
    final var backends = this.backends;
    return backends.get(random.nextInt(backends.size()));
  }

  public synchronized void refresh(boolean force) {
    final var current = System.currentTimeMillis();
    if (current - preConnectionTs >= DEFAULT_REFRESH_MILLS || force) {
      preConnectionTs = current;
      final var newBackends = sortedActiveBackends(fenodes, username, password);
      if (diff(backends, newBackends)) {
        LOG.info("be list changed, old {}, new {}", backends, newBackends);
      }
      this.backends = newBackends;
    }
  }

  /**
   * create and init schedule executor service.
   *
   * @return ScheduledExecutorService
   */
  private ScheduledExecutorService createAndInitRefreshService() {
    final var beRefreshExecutorService = Executors.newSingleThreadScheduledExecutor();
    beRefreshExecutorService.scheduleWithFixedDelay(
        () -> {
          try {
            refresh(false);
          } catch (Exception ignore) {
          }
        },
        DEFAULT_REFRESH_MILLS,
        DEFAULT_REFRESH_MILLS,
        TimeUnit.MILLISECONDS);
    return beRefreshExecutorService;
  }

  public String loadUrlStr() {
    return "http://" + random() + "/api/" + db + "/" + tbl + "/_stream_load?";
  }

  /**
   * get sorted active backends.
   *
   * @param fenodes fenodes
   * @param username username
   * @param password password
   * @return sorted list
   */
  private static List<String> sortedActiveBackends(
      String fenodes, String username, String password) {
    try {
      return getBackendsV2(fenodes, username, password).stream()
          .sorted()
          .collect(Collectors.toList());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    if (beRefreshExecutorService != null) {
      beRefreshExecutorService.shutdown();
    }
  }
}
