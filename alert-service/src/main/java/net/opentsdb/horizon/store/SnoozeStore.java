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

import net.opentsdb.horizon.model.Snooze;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static net.opentsdb.horizon.converter.AlertConverter.NOTIFICATION;
import static net.opentsdb.horizon.converter.AlertConverter.RECIPIENTS;
import static net.opentsdb.horizon.util.Utils.deSerialize;
import static net.opentsdb.horizon.util.Utils.serialize;
import static java.util.Collections.EMPTY_MAP;

public class SnoozeStore extends BaseStore {

  public SnoozeStore(DataSource rwSrc, DataSource roSrc) {
    super(rwSrc, roSrc);
  }

  public static final String SQL_ADD_SNOOZE =
      "INSERT INTO snooze(definition, starttime, endtime, enabled, deleted, namespaceid, "
          + "createdby, createdtime,updatedby, updatedtime) "
          + "VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

  public int[] creates(List<Snooze> snoozes, Connection connection)
      throws SQLException, IOException {

    try (PreparedStatement statement =
        connection.prepareStatement(SQL_ADD_SNOOZE, Statement.RETURN_GENERATED_KEYS)) {
      for (Snooze snooze : snoozes) {
        statement.setBytes(1, serialize(snooze.getDefinition()).getBytes());
        statement.setTimestamp(2, snooze.getStartTime());
        statement.setTimestamp(3, snooze.getEndTime());
        statement.setBoolean(4, snooze.isEnabled());
        statement.setBoolean(5, snooze.isDeleted());
        statement.setInt(6, snooze.getNamespaceId());
        statement.setString(7, snooze.getCreatedBy());
        statement.setTimestamp(8, snooze.getCreatedTime());
        statement.setString(9, snooze.getUpdatedBy());
        statement.setTimestamp(10, snooze.getUpdatedTime());
        statement.addBatch();
      }

      int[] result = statement.executeBatch();
      if (snoozes.isEmpty()) {
        return result;
      } else {
        final ResultSet keys = statement.getGeneratedKeys();
        int i = 0;
        while (keys.next()) {
          final long key = keys.getLong(1);
          snoozes.get(i++).setId(key);
        }
      }
      return result;
    }
  }

  public static final String SQL_ADD_SNOOZE_CONTACT =
      "INSERT INTO snooze_contact(snoozeid, contactid) VALUES (?, ?)";

  public int[] createSnoozeContact(long snoozeid, List<Integer> contactids, Connection connection)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(SQL_ADD_SNOOZE_CONTACT)) {
      for (int contactid : contactids) {
        statement.setLong(1, snoozeid);
        statement.setInt(2, contactid);
        statement.addBatch();
      }
      return statement.executeBatch();
    }
  }

  public static final String SQL_DELETE_SNOOZE_CONTACT_BY_SNOOZE =
      "DELETE FROM snooze_contact WHERE snoozeid = ?";

  public int deleteSnoozeContactBySnoozeId(long snoozeid, Connection connection)
      throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(SQL_DELETE_SNOOZE_CONTACT_BY_SNOOZE)) {
      statement.setLong(1, snoozeid);
      int result = statement.executeUpdate();
      return result;
    }
  }

  public int[] deleteSnoozeContactBySnoozeId(long[] snoozeIds, Connection connection)
      throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(SQL_DELETE_SNOOZE_CONTACT_BY_SNOOZE)) {
      for (long id : snoozeIds) {
        statement.setLong(1, id);
        statement.addBatch();
      }
      return statement.executeBatch();
    }
  }

  public static final String SQL_GET = "SELECT * FROM snooze where namespaceid = ? and deleted = ?";

  public List<Snooze> getForNamespace(int namespaceid, boolean deleted, Connection connection)
      throws SQLException, IOException {
    List<Snooze> snoozes = new ArrayList<>();
    try (PreparedStatement statement = connection.prepareStatement(SQL_GET)) {
      statement.setInt(1, namespaceid);
      statement.setBoolean(2, deleted);
      final ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        snoozes.add(resultSetToSnooze(resultSet));
      }
    }
    return snoozes;
  }

  public static final String SQL_GET_BY_ID = "SELECT * FROM snooze WHERE id = ? AND deleted = ?";

  public Snooze getById(long id, boolean deleted, Connection connection)
      throws SQLException, IOException {
    Snooze snooze = null;
    try (PreparedStatement statement = connection.prepareStatement(SQL_GET_BY_ID)) {
      statement.setLong(1, id);
      statement.setBoolean(2, deleted);
      final ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        snooze = resultSetToSnooze(resultSet);
      }
    }
    return snooze;
  }

  private static final String SQL_UPDATE_SNOOZE =
      "UPDATE snooze SET definition = ?,starttime = ?, endtime = ?, enabled = ?, deleted = ?, "
          + "updatedby = ?, updatedtime = ? WHERE id = ? ";

  public int[] update(final List<Snooze> snoozes, Connection connection)
      throws SQLException, IOException {
    try (PreparedStatement statement = connection.prepareStatement(SQL_UPDATE_SNOOZE)) {
      for (Snooze snooze : snoozes) {
        statement.setBytes(1, serialize(snooze.getDefinition()).getBytes());
        statement.setTimestamp(2, snooze.getStartTime());
        statement.setTimestamp(3, snooze.getEndTime());
        statement.setBoolean(4, snooze.isEnabled());
        statement.setBoolean(5, snooze.isDeleted());
        statement.setString(6, snooze.getUpdatedBy());
        statement.setTimestamp(7, snooze.getUpdatedTime());
        statement.setLong(8, snooze.getId());
        statement.addBatch();
      }
      return statement.executeBatch();
    }
  }

  private static final String SQL_DELETE = "DELETE FROM snooze WHERE id = ?";

  public int[] delete(final long[] ids, Connection connection) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(SQL_DELETE)) {
      for (long id : ids) {
        statement.setLong(1, id);
        statement.addBatch();
      }
      return statement.executeBatch();
    }
  }

  public static final Snooze resultSetToSnooze(ResultSet resultSet) throws SQLException, IOException {

    Map<String, Object> definition = deSerialize(resultSet.getBytes("definition"), Map.class);

    Object contact =
        ((Map<String, Object>) Optional.ofNullable(definition.get(NOTIFICATION)).orElse(EMPTY_MAP))
            .get(RECIPIENTS);

    final Snooze snooze =
        Snooze.builder()
            .id(resultSet.getLong("id"))
            .definition(definition)
            .startTime(resultSet.getTimestamp("starttime"))
            .endTime(resultSet.getTimestamp("endtime"))
            .namespaceId(resultSet.getInt("namespaceid"))
            .enabled(resultSet.getBoolean("enabled"))
            .deleted(resultSet.getBoolean("deleted"))
            .contact(contact)
            .build();

    snooze.setCreatedTime(resultSet.getTimestamp("createdtime"));
    snooze.setCreatedBy(resultSet.getString("createdby"));
    snooze.setUpdatedTime(resultSet.getTimestamp("updatedtime"));
    snooze.setUpdatedBy(resultSet.getString("updatedby"));
    return snooze;
  }
}
