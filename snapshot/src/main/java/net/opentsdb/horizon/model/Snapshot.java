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

import javax.persistence.Transient;
import java.sql.Timestamp;

public class Snapshot extends CommonModel {

  private long id;
  private String name;
  private byte sourceType;
  private long sourceId;
  private byte[] contentId;

  private Object content;

  /** Read from the source entity table for the get by snapshot id. */
  @Transient private String sourceName;

  /** Read from the activity for the get recently visited snapshots. */
  @Transient private Timestamp lastVisitedTime;

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public byte getSourceType() {
    return sourceType;
  }

  public void setSourceType(byte sourceType) {
    this.sourceType = sourceType;
  }

  public long getSourceId() {
    return sourceId;
  }

  public void setSourceId(long sourceId) {
    this.sourceId = sourceId;
  }

  public byte[] getContentId() {
    return contentId;
  }

  public void setContentId(byte[] contentId) {
    this.contentId = contentId;
  }

  public Object getContent() {
    return content;
  }

  public void setContent(Object content) {
    this.content = content;
  }

  public String getSourceName() {
    return sourceName;
  }

  public void setSourceName(String sourceName) {
    this.sourceName = sourceName;
  }

  public Timestamp getLastVisitedTime() {
    return lastVisitedTime;
  }

  public void setLastVisitedTime(Timestamp lastVisitedTime) {
    this.lastVisitedTime = lastVisitedTime;
  }
}
