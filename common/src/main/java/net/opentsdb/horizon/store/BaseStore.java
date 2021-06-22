/*
 * This file is part of OpenTSDB.
 *  Copyright (C) 2021 Yahoo.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express  implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.opentsdb.horizon.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class BaseStore {

  protected final Logger LOGGER = LoggerFactory.getLogger(getClass());

  private DataSource rwSrc;
  private DataSource roSrc;

  public BaseStore(final DataSource rwSrc, final DataSource roSrc) {
    this.rwSrc = rwSrc;
    this.roSrc = roSrc;

    if (this.roSrc == null) {
      this.roSrc = rwSrc;
    }
  }

  public Connection getReadOnlyConnection() throws SQLException {
    return getConnection(false, false);
  }

  public Connection getReadWriteConnection() throws SQLException {
    return getConnection(false, true);
  }

  protected Connection getConnection(final boolean autoCommit, final boolean readWrite)
      throws SQLException {
    DataSource src = readWrite ? rwSrc : roSrc;
    try {
      Connection connection = src.getConnection();
      connection.setAutoCommit(autoCommit);
      return connection;
    } catch (SQLException e) {
      String message = "Error getting connection to database r/w: " + readWrite;
      LOGGER.error(message, e);
      throw e;
    }
  }

  public void rollback(final Connection connection) throws SQLException {
    try {
      connection.rollback();
    } catch (SQLException e) {
      LOGGER.error("Error rollback changes", e);
      throw e;
    } finally {
      rollbackAutoCommit(connection);
    }
  }

  public void commit(final Connection connection) throws SQLException {
    try {
      connection.commit();
    } catch (SQLException e) {
      LOGGER.error("Error committing changes", e);
      throw e;
    } finally {
      rollbackAutoCommit(connection);
    }
  }

  private void rollbackAutoCommit(Connection connection) throws SQLException {
    try {
      connection.setAutoCommit(true);
    } catch (SQLException e) {
      LOGGER.error("Error rollback auto-commit", e);
      throw e;
    }
  }

  public String formatErrorMessage(SQLException e) {
    String message = e.getMessage();
    LOGGER.error(
        "SQL errorCode: {} state: {} message: {}", e.getErrorCode(), e.getSQLState(), message);
    return message;
  }

  protected String getIndexName(String message) {
    return message.substring(
        message.indexOf("for key '") + "for key '".length(), message.length() - 1);
  }
}
