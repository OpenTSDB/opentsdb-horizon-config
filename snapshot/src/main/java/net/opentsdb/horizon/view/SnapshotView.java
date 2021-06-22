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

package net.opentsdb.horizon.view;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.sql.Timestamp;

import static net.opentsdb.horizon.converter.BaseConverter.NOT_PASSED;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SnapshotView extends BaseDto {

  private long id;
  private String name;
  private SourceType sourceType;
  private long sourceId = NOT_PASSED;
  private Object content;
  private String path;

  private String sourceName;
  private Timestamp lastVisitedTime;

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

  public SourceType getSourceType() {
    return sourceType;
  }

  public void setSourceType(SourceType sourceType) {
    this.sourceType = sourceType;
  }

  public long getSourceId() {
    return sourceId;
  }

  public void setSourceId(long sourceId) {
    this.sourceId = sourceId;
  }

  public Object getContent() {
    return content;
  }

  public void setContent(Object content) {
    this.content = content;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
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
