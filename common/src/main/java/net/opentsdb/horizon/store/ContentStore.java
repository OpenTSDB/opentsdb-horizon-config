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

import net.opentsdb.horizon.model.Content;
import net.opentsdb.horizon.model.ContentHistory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class ContentStore extends BaseStore {
  public ContentStore(DataSource rwSrc, DataSource roSrc) {
    super(rwSrc, roSrc);
  }

  private static final String CREATE_CONTENT_SQL =
      "INSERT INTO content (sha2, data, createdtime, createdby) SELECT ?, ?, ?, ? FROM (SELECT 1) l LEFT JOIN content r ON r.sha2 = ? WHERE r.sha2 IS NULL";

  public void createContent(Connection connection, Content content) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(CREATE_CONTENT_SQL)) {
      statement.setBytes(1, content.getSha2());
      statement.setBytes(2, content.getData());
      statement.setTimestamp(3, content.getCreatedTime());
      statement.setString(4, content.getCreatedBy());
      statement.setBytes(5, content.getSha2());
      statement.executeUpdate();
    }
  }

  private static final String CREATE_CONTENT_HISTORY_SQL =
      "INSERT INTO content_history (contenttype, entityid, contentid, createdby, createdtime) SELECT ?, ?, ?, ?, ? FROM (SELECT 1) l "
          + "LEFT JOIN content_history r ON r.contenttype = ? AND r.entityid = ? AND r.contentid = ? AND r.createdby = ? AND r.createdtime = ? "
          + "WHERE r.contenttype IS NULL AND r.entityid IS NULL AND r.contentid IS NULL AND r.createdby IS NULL AND r.createdtime IS NULL";

  public long createContentHistory(Connection connection, ContentHistory history)
      throws SQLException {

    long id = -1;
    try (PreparedStatement statement =
        connection.prepareStatement(CREATE_CONTENT_HISTORY_SQL, Statement.RETURN_GENERATED_KEYS)) {
      statement.setByte(1, history.getContentType());
      statement.setLong(2, history.getEntityId());
      statement.setBytes(3, history.getContentId());
      statement.setString(4, history.getCreatedBy());
      statement.setTimestamp(5, history.getCreatedTime());
      statement.setByte(6, history.getContentType());
      statement.setLong(7, history.getEntityId());
      statement.setBytes(8, history.getContentId());
      statement.setString(9, history.getCreatedBy());
      statement.setTimestamp(10, history.getCreatedTime());
      statement.executeUpdate();

      final ResultSet generatedKeys = statement.getGeneratedKeys();
      while (generatedKeys.next()) {
        id = generatedKeys.getLong(1);
      }
    }
    return id;
  }

  private static final String GET_CONTENT_HISTORY_SQL =
      "select * from content_history where contenttype = ? AND entityid = ?";

  public List<ContentHistory> getContentHistory(
      Connection connection, byte contentType, long entityId) throws SQLException {
    List<ContentHistory> list = new ArrayList<>();
    try (PreparedStatement statement = connection.prepareStatement(GET_CONTENT_HISTORY_SQL)) {
      statement.setByte(1, contentType);
      statement.setLong(2, entityId);
      final ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        list.add(resultSetToContentHistory(resultSet));
      }
    }
    return list;
  }

  private static ContentHistory resultSetToContentHistory(ResultSet resultSet) throws SQLException {
    ContentHistory history = new ContentHistory();
    history.setId(resultSet.getLong("id"));
    history.setContentType(resultSet.getByte("contenttype"));
    history.setEntityId(resultSet.getLong("entityid"));
    history.setContentId(resultSet.getBytes("contentid"));
    history.setCreatedBy(resultSet.getString("createdby"));
    history.setCreatedTime(resultSet.getTimestamp("createdtime"));
    return history;
  }
}
