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

import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.model.User;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class NamespaceFollowerStore extends BaseStore {

  public NamespaceFollowerStore(final DataSource rwSrc, final DataSource roSrc) {
    super(rwSrc, roSrc);
  }

  private static final String SQL_ADD_FOLLOWERS =
      "INSERT INTO namespace_follower (namespaceid, userid) SELECT ?, ? FROM (SELECT 1) l LEFT JOIN namespace_follower r ON r.namespaceid = ? AND r.userid = ? WHERE r.namespaceid IS NULL AND r.userid IS NULL";

  public void addFollowers(
      final int namespaceid, final List<String> followerIdList, Connection connection)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(SQL_ADD_FOLLOWERS)) {
      for (String followerId : followerIdList) {
        statement.setInt(1, namespaceid);
        statement.setString(2, followerId);
        statement.setInt(3, namespaceid);
        statement.setString(4, followerId);
        statement.addBatch();
      }
      statement.executeBatch();
    }
  }

  private static final String SQL_REMOVE_FOLLOWERS =
      "DELETE FROM namespace_follower WHERE namespaceid = ? AND userid = ?";

  public void removeFollowers(
      final int namespaceid, final List<String> followerIdList, Connection connection)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(SQL_REMOVE_FOLLOWERS)) {
      for (String followerId : followerIdList) {
        statement.setInt(1, namespaceid);
        statement.setString(2, followerId);
        statement.addBatch();
      }
      statement.executeBatch();
    }
  }

  public int[] removeFollowers(
      final List<Integer> namespaceIds, final List<String> followerIds, final Connection connection)
      throws SQLException {
    int[] counts;
    try (PreparedStatement statement = connection.prepareStatement(SQL_REMOVE_FOLLOWERS)) {
      for (int namespaceId : namespaceIds) {
        for (String followerId : followerIds) {
          statement.setInt(1, namespaceId);
          statement.setString(2, followerId);
          statement.addBatch();
        }
      }
      counts = statement.executeBatch();
    }
    return counts;
  }

  private static final String SQL_GET_FOLLOWERS =
      "SELECT u.userid, u.name, u.enabled FROM namespace_follower nm "
          + "INNER JOIN user u ON u.userid = nm.userid "
          + "WHERE nm.namespaceid = ?";

  public List<User> getNamespaceFollowers(int namespaceid, Connection connection)
      throws SQLException {
    List<User> followerList;
    try (PreparedStatement statement = connection.prepareStatement(SQL_GET_FOLLOWERS)) {
      statement.setInt(1, namespaceid);
      final ResultSet resultSet = statement.executeQuery();
      followerList = ResultSetMapper.resultSetToUserListMapper(resultSet);
    }
    return followerList;
  }

  public List<Namespace> getFollowingNamespaces(String userId, Connection connection)
      throws SQLException {
    String sql =
        "SELECT n.id, n.name, n.alias FROM namespace n "
            + "INNER JOIN namespace_follower nm ON n.id = nm.namespaceid "
            + "WHERE nm.userid = ?";

    List<Namespace> namespaceList = new ArrayList();
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, userId);
      final ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        Namespace namespace = new Namespace();
        namespace.setId(resultSet.getInt("id"));
        namespace.setName(resultSet.getString("name"));
        namespace.setAlias(resultSet.getString("alias"));
        namespaceList.add(namespace);
      }
    }
    return namespaceList;
  }
}
