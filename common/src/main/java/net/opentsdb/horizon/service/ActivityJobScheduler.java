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

package net.opentsdb.horizon.service;

import net.opentsdb.horizon.store.ActivityStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;

public class ActivityJobScheduler {

  private static Logger logger = LoggerFactory.getLogger(ActivityJobScheduler.class);

  private ActivityStore activityStore;
  private ExecutorService executorService;

  public ActivityJobScheduler(
      final ActivityStore activityStore, final ExecutorService executorService) {
    this.activityStore = activityStore;
    this.executorService = executorService;
  }

  public void addActivity(final String userId, final byte entityType, final long entityId) {
    executorService.submit(
        () -> {
          try (Connection connection = activityStore.getReadWriteConnection()) {
            activityStore.addActivity(connection, userId, entityType, entityId);
            activityStore.commit(connection);
          } catch (SQLException e) {
            logger.error(
                "Error adding user activity for user: "
                    + userId
                    + " entityType : "
                    + entityType
                    + " entityId : "
                    + entityId
                    + "cause: "
                    + e.getMessage());
          }
        });
  }
}
