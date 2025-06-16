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

package name.zicat.astatine.connector.doris.util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DorisJDBCUtils. */
public class DorisJDBCUtils {

  private static final Logger LOG = LoggerFactory.getLogger(DorisJDBCUtils.class);
  private static final String URL = "jdbc:mysql:loadbalance://%s/%s";
  private static final String META_DB_NAME = "information_schema";
  private static final String META_BACKENDS_SQL = "SHOW BACKENDS";

  /**
   * get backends v2.
   *
   * @param fenodes fenodes
   * @param username username
   * @param password password
   * @return list
   * @throws SQLException SQLException
   */
  public static List<String> getBackendsV2(String fenodes, String username, String password)
      throws SQLException {
    final var beList = new ArrayList<String>();
    querySql(
        fenodes,
        META_DB_NAME,
        username,
        password,
        META_BACKENDS_SQL,
        rs -> {
          while (rs.next()) {
            if ("true".equals(rs.getString("Alive")) && "mix".equals(rs.getString("NodeRole"))) {
              beList.add(rs.getString("Host") + ":" + rs.getInt("HttpPort"));
            }
          }
        });
    if (beList.isEmpty()) {
      throw new RuntimeException("be list not found");
    }
    return beList;
  }

  /**
   * query sql.
   *
   * @param hostPorts hostPorts
   * @param db db
   * @param userName userName
   * @param password password
   * @param sql sql
   * @param handler handler
   * @throws SQLException SQLException
   */
  public static void querySql(
      String hostPorts,
      String db,
      String userName,
      String password,
      String sql,
      ResultSetHandler handler)
      throws SQLException {
    final var url = String.format(URL, hostPorts, db);
    try (var connection = DriverManager.getConnection(url, userName, password);
        var statement = connection.createStatement();
        var rs = statement.executeQuery(sql)) {
      handler.executor(rs);
    }
  }

  public static void executeSql(
      String hostPorts, String db, String userName, String password, String sql)
      throws SQLException {
    final var url = String.format(URL, hostPorts, db);
    LOG.info("start execute sql by user {}: {}", userName, sql);
    try (var connection = DriverManager.getConnection(url, userName, password);
        var statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  /** ResultSetHandler. */
  public interface ResultSetHandler {

    /**
     * rs.
     *
     * @param rs rs
     */
    void executor(ResultSet rs) throws SQLException;
  }
}
