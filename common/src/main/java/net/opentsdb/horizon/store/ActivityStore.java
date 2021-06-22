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

import net.opentsdb.horizon.service.BaseService;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class ActivityStore extends BaseStore {

  public ActivityStore(DataSource rwSrc, DataSource roSrc) {
    super(rwSrc, roSrc);
  }

  public int addActivity(
      final Connection connection, final String userId, final byte entityType, final long entityId)
      throws SQLException {
    String sql =
        "INSERT INTO activity (userid, entitytype, entityid, timestamp) values (?, ?, ?, ?) "
            + "ON DUPLICATE KEY UPDATE timestamp = ?";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      Timestamp now = BaseService.now();
      statement.setString(1, userId);
      statement.setByte(2, entityType);
      statement.setLong(3, entityId);
      statement.setTimestamp(4, now);
      statement.setTimestamp(5, now);
      return statement.executeUpdate();
    }
  }
}
