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

public class NamespaceMemberStore extends BaseStore {

  public NamespaceMemberStore(final DataSource rwSrc, final DataSource roSrc) {
    super(rwSrc, roSrc);
  }

  private static final String SQL_ADD_MEMBERS =
      "INSERT INTO namespace_member (namespaceid, userid) SELECT ?, ? FROM (SELECT 1) l LEFT JOIN namespace_member r ON r.namespaceid = ? AND r.userid = ? WHERE r.namespaceid IS NULL AND r.userid IS NULL";

  public int[] addMembers(
      final int namespaceid, final List<String> memberIdList, Connection connection)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(SQL_ADD_MEMBERS)) {
      for (String memberId : memberIdList) {
        statement.setInt(1, namespaceid);
        statement.setString(2, memberId);
        statement.setInt(3, namespaceid);
        statement.setString(4, memberId);
        statement.addBatch();
      }
      return statement.executeBatch();
    }
  }

  public int[] addMembers(
      final List<Integer> namespaceIds,
      final List<String> memberIdList,
      final Connection connection)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(SQL_ADD_MEMBERS)) {
      for (int namespaceId : namespaceIds) {
        for (String memberId : memberIdList) {
          statement.setInt(1, namespaceId);
          statement.setString(2, memberId);
          statement.addBatch();
        }
      }
      return statement.executeBatch();
    }
  }

  private static final String SQL_REMOVE_MEMBERS =
      "DELETE FROM namespace_member WHERE namespaceid = ? AND userid = ?";

  public int[] removeNamespaceMember(
      final int namespaceid, final List<String> memberIdList, Connection connection)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(SQL_REMOVE_MEMBERS)) {
      for (String memberId : memberIdList) {
        statement.setInt(1, namespaceid);
        statement.setString(2, memberId);
        statement.addBatch();
      }
      return statement.executeBatch();
    }
  }

  private static final String SQL_REMOVE_ALL_MEMBERS =
      "DELETE FROM namespace_member WHERE namespaceid = ?";

  public int removeAllNamespaceMember(final int namespaceid, Connection connection)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(SQL_REMOVE_ALL_MEMBERS)) {
      statement.setInt(1, namespaceid);
      return statement.executeUpdate();
    }
  }

  private static final String SQL_GET_MEMBERS =
      "SELECT u.userid, u.name, u.enabled FROM namespace_member nm "
          + "INNER JOIN user u ON u.userid = nm.userid "
          + "WHERE nm.namespaceid = ?";

  public List<User> getNamespaceMembers(int namespaceid, Connection connection)
      throws SQLException {
    List<User> membersList;
    try (PreparedStatement statement = connection.prepareStatement(SQL_GET_MEMBERS)) {
      statement.setInt(1, namespaceid);
      final ResultSet resultSet = statement.executeQuery();
      membersList = ResultSetMapper.resultSetToUserListMapper(resultSet);
    }
    return membersList;
  }

  public List<String> getMemberIdsByAlias(String alias, Connection connection) throws SQLException {
    String sql =
        "SELECT nm.userid FROM namespace_member nm "
            + "INNER JOIN namespace n ON nm.namespaceid = n.id "
            + "WHERE n.alias = ?";

    List<String> userIds = new ArrayList();
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, alias);
      final ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        userIds.add(resultSet.getString("userid"));
      }
    }
    return userIds;
  }

  public List<Namespace> getMemberNamespaces(String userId, Connection connection)
      throws SQLException {
    String sql =
        "SELECT n.id, n.name, n.alias FROM namespace n "
            + "INNER JOIN namespace_member nm ON n.id = nm.namespaceid "
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
