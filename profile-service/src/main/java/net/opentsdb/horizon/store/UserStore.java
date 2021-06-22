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

import net.opentsdb.horizon.model.User;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static net.opentsdb.horizon.store.ResultSetMapper.userMapper;

public class UserStore extends BaseStore {

  public UserStore(final DataSource rwSrc, final DataSource roSrc) {
    super(rwSrc, roSrc);
  }

  @Override
  public String formatErrorMessage(SQLException e) {
    int errorCode = e.getErrorCode();
    String message = e.getMessage();
    if (errorCode == 1062) {
      int valueStartIndex = message.indexOf("'");
      String value =
          message.substring(valueStartIndex + 1, message.indexOf("'", valueStartIndex + 1));
      String indexName = getIndexName(message);
      String field = getField(indexName);
      return "Duplicate user " + field + ": " + value;
    } else {
      return super.formatErrorMessage(e);
    }
  }

  private String getField(String indexName) {
    if (indexName.equals("user.PRIMARY")) {
      return "id";
    } else {
      return indexName;
    }
  }

  String SQL_INSERT_USER =
      "INSERT INTO user (userid, name, enabled, creationmode, updatedtime) VALUES (?, ?, ?, ?, ?)";

  public int create(final User user, final Connection connection) throws SQLException {

    try (PreparedStatement statement = connection.prepareStatement(SQL_INSERT_USER)) {
      statement.setString(1, user.getUserid());
      statement.setString(2, user.getName());
      statement.setBoolean(3, user.isEnabled());
      statement.setByte(4, user.getCreationmode().id);
      statement.setTimestamp(5, user.getUpdatedtime());
      return statement.executeUpdate();
    }
  }

  public int[] create(final List<User> users, final Connection connection) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(SQL_INSERT_USER)) {
      for (User user : users) {
        statement.setString(1, user.getUserid());
        statement.setString(2, user.getName());
        statement.setBoolean(3, user.isEnabled());
        statement.setByte(4, user.getCreationmode().id);
        statement.setTimestamp(5, user.getUpdatedtime());

        statement.addBatch();
      }
      return statement.executeBatch();
    }
  }

  public int[] createOrUpdate(final List<User> users, final Connection connection)
      throws SQLException {
    String sql =
        "INSERT INTO user (userid, name, enabled, creationmode, updatedtime) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE name = ?";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {

      for (User user : users) {
        statement.setString(1, user.getUserid());
        statement.setString(2, user.getName());
        statement.setBoolean(3, user.isEnabled());
        statement.setByte(4, user.getCreationmode().id);
        statement.setTimestamp(5, user.getUpdatedtime());
        statement.setString(6, user.getName());
        statement.addBatch();
      }
      return statement.executeBatch();
    }
  }

  public User getNameById(final String id, final Connection connection) throws SQLException {
    String sql = "SELECT userid, name FROM user WHERE userid = ?";

    User user = null;
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, id);
      final ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        user = new User();
        user.setUserid(resultSet.getString("userid"));
        user.setName(resultSet.getString("name"));
      }
    }
    return user;
  }

  public User getById(final String id, final Connection connection) throws SQLException {
    String sql = "SELECT * FROM user WHERE userid = ?";

    User user = null;
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, id);
      final ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        user = userMapper(resultSet);
      }
    }
    return user;
  }

  public List<User> getAll(Connection connection) throws SQLException {
    String sql = "SELECT userid, name, enabled FROM user";

    List<User> userList = new ArrayList();
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      final ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        User user = new User();
        user.setUserid(resultSet.getString("userid"));
        user.setName(resultSet.getString("name"));
        user.setEnabled(resultSet.getBoolean("enabled"));
        userList.add(user);
      }
    }
    return userList;
  }

  public List<User> getUsers(Connection connection, boolean enabled) throws SQLException {
    String sql = "SELECT userid, name FROM user WHERE enabled = ?";

    List<User> userList = new ArrayList();
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setBoolean(1, enabled);
      final ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        User user = new User();
        user.setUserid(resultSet.getString("userid"));
        user.setName(resultSet.getString("name"));
        userList.add(user);
      }
    }
    return userList;
  }
}
