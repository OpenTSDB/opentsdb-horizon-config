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

import net.opentsdb.horizon.model.Alert;
import net.opentsdb.horizon.model.AlertType;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static net.opentsdb.horizon.converter.AlertConverter.VERSION;
import static net.opentsdb.horizon.converter.BaseConverter.NOT_PASSED;
import static net.opentsdb.horizon.service.AlertService.DEFAULT_VERSION;
import static net.opentsdb.horizon.service.BaseService.now;
import static net.opentsdb.horizon.util.Utils.deSerialize;
import static net.opentsdb.horizon.util.Utils.serialize;

public class AlertStore extends BaseStore {

  public AlertStore(DataSource rwSrc, DataSource roSrc) {
    super(rwSrc, roSrc);
  }

  @Override
  public String formatErrorMessage(SQLException e) {
    int errorCode = e.getErrorCode();
    String message = e.getMessage();
    if (errorCode == 1062) {
      int valueStartIndex = message.indexOf('-');
      String value =
          message.substring(valueStartIndex + 1, message.indexOf("'", valueStartIndex + 1));
      String field = getField(getIndexName(message));
      return "Duplicate alert " + field + ": " + value;
    } else {
      return super.formatErrorMessage(e);
    }
  }

  private String getField(String indexKey) {
    if (indexKey.equals("alert.uq_namespace_name")) {
      return "name";
    } else {
      return indexKey;
    }
  }

  public static final String SQL_ADD_ALERT =
      "INSERT INTO alert(name, type, labels, definition, enabled, deleted, namespaceid, createdby, createdtime, "
          + "updatedby, updatedtime) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

  public int[] create(List<Alert> alerts, Connection connection) throws SQLException, IOException {

    try (PreparedStatement statement =
        connection.prepareStatement(SQL_ADD_ALERT, Statement.RETURN_GENERATED_KEYS)) {
      for (Alert alert : alerts) {
        statement.setString(1, alert.getName());
        statement.setByte(2, alert.getType().getId());
        statement.setBytes(3, serialize(alert.getLabels()).getBytes());
        statement.setBytes(4, serialize(alert.getDefinition()).getBytes());
        statement.setBoolean(5, alert.isEnabled());
        statement.setBoolean(6, alert.isDeleted());
        statement.setInt(7, alert.getNamespaceId());
        statement.setString(8, alert.getCreatedBy());
        statement.setTimestamp(9, alert.getCreatedTime());
        statement.setString(10, alert.getUpdatedBy());
        statement.setTimestamp(11, alert.getUpdatedTime());
        statement.addBatch();
      }
      int[] result = statement.executeBatch();
      if (!alerts.isEmpty()) {
        final ResultSet generatedKeys = statement.getGeneratedKeys();
        int i = 0;
        while (generatedKeys.next()) {
          final int id = generatedKeys.getInt(1);
          alerts.get(i++).setId(id);
        }
      }
      return result;
    }
  }

  public static final String SQL_ADD_ALERT_CONTACT =
      "INSERT INTO alert_contact(alertid, contactid) VALUES (?, ?)";

  public int[] createAlertContact(long alertId, List<Integer> contactIds, Connection connection)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(SQL_ADD_ALERT_CONTACT)) {
      for (Integer contactId : contactIds) {
        statement.setLong(1, alertId);
        statement.setInt(2, contactId);
        statement.addBatch();
      }
      return statement.executeBatch();
    }
  }

  private static final String SQL_GET_ALERT_CONTACT =
      "SELECT contactid FROM alert_contact WHERE alertid = ?";

  public List<Integer> getContactIds(final long alertId, final Connection connection)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(SQL_GET_ALERT_CONTACT)) {
      statement.setLong(1, alertId);
      final ResultSet resultSet = statement.executeQuery();
      List<Integer> contactIds = new ArrayList<>();
      while (resultSet.next()) {
        contactIds.add(resultSet.getInt("contactid"));
      }
      return contactIds;
    }
  }

  private static final String SQL_DELETE_CONTACTS_FROM_ALERT =
      "DELETE FROM alert_contact WHERE alertid = ? AND contactid = ?";

  public int[] deleteAlertContactByAlert(
      final long alertId, List<Integer> contactIds, Connection connection) throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(SQL_DELETE_CONTACTS_FROM_ALERT)) {
      for (int i = 0; i < contactIds.size(); i++) {
        Integer contactId = contactIds.get(i);
        statement.setLong(1, alertId);
        statement.setInt(2, contactId);
        statement.addBatch();
      }
      return statement.executeBatch();
    }
  }

  private static final String SQL_REMOVE_ALL_CONTACTS_FROM_ALERTS =
      "DELETE FROM alert_contact WHERE alertid = ?";

  public int[] removeContactsFromAlert(final long[] alertIds, final Connection connection)
      throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(SQL_REMOVE_ALL_CONTACTS_FROM_ALERTS)) {
      for (int i = 0; i < alertIds.length; i++) {
        long alertId = alertIds[i];
        statement.setLong(1, alertId);
        statement.addBatch();
      }
      return statement.executeBatch();
    }
  }

  public static final String SQL_GET_BY_NAME =
      "SELECT * FROM alert WHERE namespaceid = ? AND name = ? AND deleted = ?";

  private static final String SQL_GET_BY_NAME_WITHOUT_DEFINITION =
      "SELECT id, name, type, enabled, deleted, namespaceid, labels, createdby, createdtime, updatedby, updatedtime "
          + "FROM alert where namespaceid = ? AND name = ? AND deleted = ?";

  public Alert get(
      int namespaceid, String name, boolean definition, boolean deleted, Connection connection)
      throws SQLException, IOException {
    String sql = definition ? SQL_GET_BY_NAME : SQL_GET_BY_NAME_WITHOUT_DEFINITION;
    Alert alert = null;
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setInt(1, namespaceid);
      statement.setString(2, name);
      statement.setBoolean(3, deleted);
      final ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        alert = resultSetToAlert(resultSet, definition);
      }
    }
    return alert;
  }

  public static final String SQL_GET_BY_NAMESPACE =
      "SELECT * FROM alert where namespaceid = ? and deleted = ?";

  private static final String SQL_GET_BY_NAMESPACE_WITHOUT_DEFINITION =
      "SELECT id, name, type, enabled, deleted, namespaceid, labels, createdby, createdtime, updatedby, updatedtime "
          + "FROM alert WHERE namespaceid = ? AND deleted = ?";

  public List<Alert> get(
      int namespaceid, boolean definition, boolean deleted, Connection connection)
      throws SQLException, IOException {
    String sql = definition ? SQL_GET_BY_NAMESPACE : SQL_GET_BY_NAMESPACE_WITHOUT_DEFINITION;
    List<Alert> alerts = new ArrayList<>();
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setInt(1, namespaceid);
      statement.setBoolean(2, deleted);
      final ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        alerts.add(resultSetToAlert(resultSet, definition));
      }
    }
    return alerts;
  }

  public static final String SQL_GET_BY_ID = "SELECT * FROM alert WHERE id = ? AND deleted = ?";

  private static final String SQL_GET_BY_ID_WITHOUT_DEFINITION =
      "SELECT id, name, type, enabled, deleted, namespaceid, labels, createdby, createdtime, updatedby, updatedtime "
          + "FROM alert WHERE id = ? AND deleted = ?";

  public Alert get(long id, boolean fetchDefinition, boolean deleted, Connection connection)
      throws SQLException, IOException {
    String sql = fetchDefinition ? SQL_GET_BY_ID : SQL_GET_BY_ID_WITHOUT_DEFINITION;
    Alert alert = null;
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setLong(1, id);
      statement.setBoolean(2, deleted);
      final ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        alert = resultSetToAlert(resultSet, fetchDefinition);
      }
    }
    return alert;
  }

  private static final String SQL_UPDATE_ALERT =
      "UPDATE alert SET name = ?, type = ?, labels = ?, definition = ?, enabled = ?, deleted = ?, namespaceid = ?, "
          + "updatedby = ?, updatedtime = ? WHERE id = ? ";

  public int[] update(final List<Alert> alerts, Connection connection)
      throws SQLException, IOException {
    try (PreparedStatement statement = connection.prepareStatement(SQL_UPDATE_ALERT)) {
      for (Alert alert : alerts) {
        statement.setString(1, alert.getName());
        statement.setByte(2, alert.getType().getId());
        statement.setBytes(3, serialize(alert.getLabels()).getBytes());
        statement.setBytes(4, serialize(alert.getDefinition()).getBytes());
        statement.setBoolean(5, alert.isEnabled());
        statement.setBoolean(6, alert.isDeleted());
        statement.setInt(7, alert.getNamespaceId());
        statement.setString(8, alert.getUpdatedBy());
        statement.setTimestamp(9, alert.getUpdatedTime());

        statement.setLong(10, alert.getId());
        statement.addBatch();
      }
      return statement.executeBatch();
    }
  }

  private static final String SQL_DELETE_ALERT = "DELETE FROM alert WHERE WHERE id = ?";

  public int[] delete(final List<Long> ids, Connection connection) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(SQL_DELETE_ALERT)) {
      for (Long id : ids) {
        statement.setLong(1, id);
        statement.addBatch();
      }
      return statement.executeBatch();
    }
  }

  private static final String SQL_SOFT_DELETE_ALERT =
      "UPDATE alert SET name = CONCAT(name, ?), deleted = ?, updatedby = ?, updatedtime = ? WHERE id = ?";

  public int[] softDelete(final long[] ids, String principal, Connection connection)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(SQL_SOFT_DELETE_ALERT)) {
      Timestamp now = now();
      for (long id : ids) {
        statement.setString(1, "-" + now.getTime());
        statement.setInt(2, 1);
        statement.setString(3, principal);
        statement.setTimestamp(4, now);
        statement.setLong(5, id);
        statement.addBatch();
      }
      return statement.executeBatch();
    }
  }

  private static final String SQL_RESTORE_ALERT =
      "UPDATE alert SET deleted = ?, updatedby = ?, updatedtime = ? WHERE id = ?";

  public int[] restore(final long[] ids, String principal, Connection connection)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(SQL_RESTORE_ALERT)) {
      Timestamp now = now();
      for (long id : ids) {
        statement.setInt(1, 0);
        statement.setString(2, principal);
        statement.setTimestamp(3, now);
        statement.setLong(4, id);
        statement.addBatch();
      }
      return statement.executeBatch();
    }
  }

  public static final Alert resultSetToAlert(ResultSet resultSet, boolean fetchDefinition)
      throws SQLException, IOException {
    Alert alert = new Alert();
    alert.setId(resultSet.getLong("id"));

    alert.setName(resultSet.getString("name"));
    alert.setNamespaceId(resultSet.getInt("namespaceid"));
    alert.setType(AlertType.getById(resultSet.getByte("type")));
    alert.setEnabled(resultSet.getBoolean("enabled"));
    alert.setDeleted(resultSet.getBoolean("deleted"));
    alert.setLabels(deSerialize(resultSet.getBytes("labels"), List.class));

    if (fetchDefinition) {
      Map<String, Object> definition = deSerialize(resultSet.getBytes("definition"), Map.class);
      alert.setVersion((Integer) definition.getOrDefault(VERSION, DEFAULT_VERSION));
      alert.setDefinition(definition);
    } else {
      alert.setVersion(NOT_PASSED);
    }

    alert.setCreatedTime(resultSet.getTimestamp("createdtime"));
    alert.setCreatedBy(resultSet.getString("createdby"));
    alert.setUpdatedTime(resultSet.getTimestamp("updatedtime"));
    alert.setUpdatedBy(resultSet.getString("updatedby"));
    return alert;
  }
}
