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

package net.opentsdb.horizon.fs.model;

import javax.persistence.Transient;
import java.sql.Timestamp;

public class FileHistory {

  private long id;
  private long fileid;
  private byte[] contentid;
  private Timestamp createdtime;
  
  @Transient private String createdBy;

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public Long getFileid() {
    return fileid;
  }

  public void setFileid(Long fileid) {
    this.fileid = fileid;
  }

  public byte[] getContentid() {
    return contentid;
  }

  public void setContentid(byte[] contentid) {
    this.contentid = contentid;
  }

  public Timestamp getCreatedtime() {
    return createdtime;
  }

  public void setCreatedtime(Timestamp createdtime) {
    this.createdtime = createdtime;
  }

  public String getCreatedBy() {
    return createdBy;
  }

  public void setCreatedBy(String createdBy) {
    this.createdBy = createdBy;
  }
}
