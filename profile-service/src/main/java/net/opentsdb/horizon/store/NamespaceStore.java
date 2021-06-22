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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static net.opentsdb.horizon.store.ResultSetMapper.resultSetToNamespace;
import static net.opentsdb.horizon.util.Utils.serialize;

public class NamespaceStore extends BaseStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(NamespaceStore.class);

  public NamespaceStore(final DataSource rwSrc, final DataSource roSrc) {
    super(rwSrc, roSrc);
  }

  private static final String SQL_CREATE_NAMESPACE =
      "INSERT INTO namespace (name, alias, meta, enabled, createdby, createdtime, updatedby, updatedtime) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

  public int create(final Namespace namespace, Connection connection)
      throws SQLException, IOException {
    int count;
    try (PreparedStatement statement =
        connection.prepareStatement(SQL_CREATE_NAMESPACE, Statement.RETURN_GENERATED_KEYS)) {
      statement.setString(1, namespace.getName());
      statement.setString(2, namespace.getAlias());
      statement.setBytes(3, serialize(namespace.getMeta()).getBytes());
      statement.setBoolean(4, namespace.getEnabled());
      statement.setString(5, namespace.getCreatedBy());
      statement.setTimestamp(6, namespace.getCreatedTime());
      statement.setString(7, namespace.getUpdatedBy());
      statement.setTimestamp(8, namespace.getUpdatedTime());
      count = statement.executeUpdate();
      final ResultSet generatedKeys = statement.getGeneratedKeys();
      while (generatedKeys.next()) {
        final int keys = generatedKeys.getInt(1);
        namespace.setId(keys);
      }
    }
    return count;
  }

  public int[] create(final List<Namespace> namespaces, Connection connection)
      throws SQLException, IOException {
    int[] counts;
    try (PreparedStatement statement =
        connection.prepareStatement(SQL_CREATE_NAMESPACE, Statement.RETURN_GENERATED_KEYS)) {

      for (Namespace namespace : namespaces) {
        statement.setString(1, namespace.getName());
        statement.setString(2, namespace.getAlias());
        statement.setBytes(3, serialize(namespace.getMeta()).getBytes());
        statement.setBoolean(4, namespace.getEnabled());
        statement.setString(5, namespace.getCreatedBy());
        statement.setTimestamp(6, namespace.getCreatedTime());
        statement.setString(7, namespace.getUpdatedBy());
        statement.setTimestamp(8, namespace.getUpdatedTime());

        statement.addBatch();
      }

      counts = statement.executeBatch();

      if (!namespaces.isEmpty()) {
        final ResultSet generatedKeys = statement.getGeneratedKeys();
        int i = 0;
        while (generatedKeys.next()) {
          final int id = generatedKeys.getInt(1);
          namespaces.get(i++).setId(id);
        }
      }
    }
    return counts;
  }

  private static final String SQL_MODIFY_NAMESPACE =
      "UPDATE namespace SET alias = ?, meta = ?, enabled = ?, updatedby = ?, updatedtime = ? WHERE id = ?";

  public Namespace update(final Namespace namespace, Connection connection)
      throws SQLException, IOException {
    try (PreparedStatement statement = connection.prepareStatement(SQL_MODIFY_NAMESPACE)) {
      setToUpdate(namespace, statement);
      statement.executeUpdate();
    }
    return namespace;
  }

  public int[] update(final List<Namespace> namespaces, Connection connection)
      throws SQLException, IOException {
    int[] counts;
    try (PreparedStatement statement = connection.prepareStatement(SQL_MODIFY_NAMESPACE)) {
      for (Namespace namespace : namespaces) {
        setToUpdate(namespace, statement);
        statement.addBatch();
      }
      counts = statement.executeBatch();
    }
    return counts;
  }

  private void setToUpdate(Namespace namespace, PreparedStatement statement)
      throws SQLException, IOException {
    statement.setString(1, namespace.getAlias());
    statement.setBytes(2, serialize(namespace.getMeta()).getBytes());
    statement.setBoolean(3, namespace.getEnabled());
    statement.setString(4, namespace.getUpdatedBy());
    statement.setTimestamp(5, namespace.getUpdatedTime());
    statement.setInt(6, namespace.getId());
  }

  public Namespace getById(int id, Connection connection) throws SQLException, IOException {
    String sql = "SELECT * FROM namespace WHERE id = ?";

    Namespace namespace = null;
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setInt(1, id);
      final ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        namespace = resultSetToNamespace(resultSet);
      }
    }
    return namespace;
  }

  public List<Namespace> getByIds(List<Integer> ids, Connection connection)
      throws SQLException, IOException {
    StringBuilder sqlBuilder = new StringBuilder("select * from namespace where id in (");
    for (int i = 0; i < ids.size(); i++) {
      if (i > 0) {
        sqlBuilder.append(", ");
      }
      sqlBuilder.append('?');
    }
    sqlBuilder.append(')');
    String sql = sqlBuilder.toString();

    List<Namespace> namespaceList = new ArrayList<>();
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      for (int i = 0; i < ids.size(); i++) {
        statement.setInt(i + 1, ids.get(i));
      }
      ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        Namespace namespace = resultSetToNamespace(resultSet);
        namespaceList.add(namespace);
      }
    }
    return namespaceList;
  }

  private static final String GET_NAMESPACE_MATCHED_BY_NAME_OR_ALIAS =
      "SELECT * FROM namespace WHERE name IN (?, ?) OR alias IN (?, ?)";

  public List<Namespace> getMatchedNamespace(String name, String alias, Connection connection)
      throws SQLException, IOException {
    List<Namespace> namespaceList = new ArrayList<>();

    try (PreparedStatement statement =
        connection.prepareStatement(GET_NAMESPACE_MATCHED_BY_NAME_OR_ALIAS)) {
      statement.setString(1, name);
      statement.setString(2, alias);
      statement.setString(3, name);
      statement.setString(4, alias);
      final ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        Namespace namespace = resultSetToNamespace(resultSet);
        namespaceList.add(namespace);
      }
    }
    return namespaceList;
  }

  private static final String GET_NAMESPACE_MATCHED_BY_NAME_OR_ALIAS_AND_EXCLUDING_NAMESPACE =
      "SELECT * FROM namespace WHERE (name IN (?, ?) OR alias IN (?, ?)) AND id != ?";

  public List<Namespace> getMatchedNamespace(
      int excludeNamespaceId, String name, String alias, Connection connection)
      throws SQLException, IOException {
    List<Namespace> namespaceList = new ArrayList<>();

    try (PreparedStatement statement =
        connection.prepareStatement(
            GET_NAMESPACE_MATCHED_BY_NAME_OR_ALIAS_AND_EXCLUDING_NAMESPACE)) {
      statement.setString(1, name);
      statement.setString(2, alias);
      statement.setString(3, name);
      statement.setString(4, alias);
      statement.setInt(5, excludeNamespaceId);
      final ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        Namespace namespace = resultSetToNamespace(resultSet);
        namespaceList.add(namespace);
      }
    }
    return namespaceList;
  }

  private static final String GET_NAMESPACE_BY_NAME_OR_ALIAS =
      "SELECT * FROM namespace WHERE name = ? OR alias = ?";

  public Namespace getNamespaceByNameOrAlias(String nameOrAlias, Connection connection)
      throws SQLException, IOException {
    Namespace namespace = null;
    try (PreparedStatement statement =
        connection.prepareStatement(GET_NAMESPACE_BY_NAME_OR_ALIAS)) {
      statement.setString(1, nameOrAlias);
      statement.setString(2, nameOrAlias);
      final ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        namespace = resultSetToNamespace(resultSet);
      }
    }
    return namespace;
  }

  private static final String GET_NAMESPACE_BY_NAME = "SELECT * FROM namespace WHERE name = ?";

  public Namespace getNamespaceByName(String name, Connection connection)
      throws SQLException, IOException {
    Namespace namespace = null;
    try (PreparedStatement statement = connection.prepareStatement(GET_NAMESPACE_BY_NAME)) {
      statement.setString(1, name);
      final ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        namespace = resultSetToNamespace(resultSet);
      }
    }
    return namespace;
  }

  private static final String GET_NAMESPACE_BY_ALIAS = "SELECT * FROM namespace WHERE alias = ?";

  public Namespace getNamespaceByAlias(String alias, Connection connection)
      throws SQLException, IOException {
    Namespace namespace = null;
    try (PreparedStatement statement = connection.prepareStatement(GET_NAMESPACE_BY_ALIAS)) {
      statement.setString(1, alias);
      final ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        namespace = resultSetToNamespace(resultSet);
      }
    }
    return namespace;
  }

  private static final String GET_ALL_NAMESPACE = "SELECT * FROM namespace";

  public List<Namespace> getAllNamespace(Connection connection) throws SQLException, IOException {
    List<Namespace> namespaceList = new ArrayList<>();
    try (PreparedStatement statement = connection.prepareStatement(GET_ALL_NAMESPACE)) {
      final ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        Namespace namespace = resultSetToNamespace(resultSet);
        namespaceList.add(namespace);
      }
    }
    return namespaceList;
  }
}
