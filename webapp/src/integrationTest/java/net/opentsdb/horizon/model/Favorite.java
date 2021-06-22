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

package net.opentsdb.horizon.model;

import java.sql.Timestamp;

public class Favorite {
  private long id;
  private long folderId;
  private String userId;
  private Timestamp createdTime;

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getFolderId() {
    return folderId;
  }

  public void setFolderId(long folderId) {
    this.folderId = folderId;
  }

  public String getUserId() {
    return userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public Timestamp getCreatedTime() {
    return createdTime;
  }

  public void setCreatedTime(Timestamp createdTime) {
    this.createdTime = createdTime;
  }
}
