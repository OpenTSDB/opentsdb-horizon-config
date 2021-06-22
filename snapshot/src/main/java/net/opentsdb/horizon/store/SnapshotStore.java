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

import net.opentsdb.horizon.model.Snapshot;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static net.opentsdb.horizon.view.SourceType.SNAPSHOT;

public class SnapshotStore extends BaseStore {
  public SnapshotStore(DataSource rwSrc, DataSource roSrc) {
    super(rwSrc, roSrc);
  }

  private static final String CREATE_SNAPSHOT =
      "INSERT INTO snapshot(name, sourcetype, sourceid, contentid, createdby, createdtime, updatedby, updatedtime) "
          + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

  public int create(final Connection connection, final Snapshot snapshot) throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(CREATE_SNAPSHOT, Statement.RETURN_GENERATED_KEYS)) {
      statement.setString(1, snapshot.getName());
      statement.setByte(2, snapshot.getSourceType());
      statement.setLong(3, snapshot.getSourceId());
      statement.setBytes(4, snapshot.getContentId());
      statement.setString(5, snapshot.getCreatedBy());
      statement.setTimestamp(6, snapshot.getCreatedTime());
      statement.setString(7, snapshot.getUpdatedBy());
      statement.setTimestamp(8, snapshot.getUpdatedTime());

      int result = statement.executeUpdate();
      final ResultSet generatedKeys = statement.getGeneratedKeys();
      while (generatedKeys.next()) {
        final int key = generatedKeys.getInt(1);
        snapshot.setId(key);
      }
      return result;
    }
  }

  private static final String UPDATE_SNAPSHOT =
      "UPDATE snapshot SET name = ?, sourcetype = ?, sourceid = ?, contentid = ?, updatedby = ?, updatedtime = ? "
          + "WHERE id = ?";

  public int update(Connection connection, Snapshot snapshot) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(UPDATE_SNAPSHOT)) {
      statement.setString(1, snapshot.getName());
      statement.setByte(2, snapshot.getSourceType());
      statement.setLong(3, snapshot.getSourceId());
      statement.setBytes(4, snapshot.getContentId());
      statement.setString(5, snapshot.getUpdatedBy());
      statement.setTimestamp(6, snapshot.getUpdatedTime());
      statement.setLong(7, snapshot.getId());
      return statement.executeUpdate();
    }
  }

  private static final String GET_SNAPSHOT_AND_CONTENT_BY_ID =
      "SELECT s.id, s.name, s.sourcetype, s.sourceid, s.contentid, s.createdby, s.createdtime, s.updatedby, s.updatedtime, c.data "
          + "FROM snapshot s INNER JOIN content c ON s.contentid = c.sha2 "
          + "WHERE s.id = ?";

  public Snapshot getSnapshotAndContentById(final long id, final Connection connection)
      throws SQLException {
    Snapshot snapshot = null;
    try (PreparedStatement statement =
        connection.prepareStatement(GET_SNAPSHOT_AND_CONTENT_BY_ID)) {
      statement.setLong(1, id);
      try (final ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
          snapshot = resultSetToSnapshot(rs);
          snapshot.setContent(rs.getBytes("data"));
        }
      }
    }
    return snapshot;
  }

  private static final String GET_BY_ID = "SELECT * FROM snapshot WHERE id = ?";

  public Snapshot getById(final Connection connection, final long id) throws SQLException {
    Snapshot snapshot = null;
    try (PreparedStatement statement = connection.prepareStatement(GET_BY_ID)) {
      statement.setLong(1, id);
      try (final ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
          snapshot = resultSetToSnapshot(rs);
        }
      }
    }
    return snapshot;
  }

  public static final String GET_RECENTLY_VISITED_SNAPSHOTS =
      "SELECT a.timestamp, s.id, s.name, "
          + "s.sourcetype, s.sourceid, s.contentid, s.createdby, s.createdtime, s.updatedby, s.updatedtime from activity a "
          + "INNER JOIN snapshot s ON a.entityid = s.id where a.userid = ? AND a.entitytype = ?"
          + " ORDER BY a.timestamp desc limit ?";

  public List<Snapshot> getRecentlyVisited(Connection connection, String userId, int limit)
      throws SQLException {

    List<Snapshot> snapshots = new ArrayList();
    try (PreparedStatement statement =
        connection.prepareStatement(GET_RECENTLY_VISITED_SNAPSHOTS)) {
      statement.setString(1, userId);
      statement.setByte(2, SNAPSHOT.id);
      statement.setInt(3, limit);
      try (final ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
          Snapshot snapshot = resultSetToSnapshot(rs);
          snapshot.setLastVisitedTime(rs.getTimestamp("timestamp"));
          snapshots.add(snapshot);
        }
      }
    }
    return snapshots;
  }

  private static Snapshot resultSetToSnapshot(ResultSet resultSet) throws SQLException {
    Snapshot snapshot = new Snapshot();
    snapshot.setId(resultSet.getLong("id"));
    snapshot.setName(resultSet.getString("name"));
    snapshot.setSourceType(resultSet.getByte("sourcetype"));
    snapshot.setSourceId(resultSet.getLong("sourceid"));
    snapshot.setContentId(resultSet.getBytes("contentid"));
    snapshot.setCreatedTime(resultSet.getTimestamp("createdtime"));
    snapshot.setCreatedBy(resultSet.getString("createdby"));
    snapshot.setUpdatedTime(resultSet.getTimestamp("updatedtime"));
    snapshot.setUpdatedBy(resultSet.getString("updatedby"));
    return snapshot;
  }
}
