/*
 * This file is part of OpenTSDB.
 * Copyright (C) 2021  Yahoo.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.opentsdb.horizon.fs.view;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class FileHistoryListDto {

  private long fileId;
  private long defaultHistoryId;
  private List<FileHistoryDto> histories;
  private Map<String, String> userNames;

  public long getFileId() {
    return fileId;
  }

  public void setFileId(long fileId) {
    this.fileId = fileId;
  }

  public long getDefaultHistoryId() {
    return defaultHistoryId;
  }

  public void setDefaultHistoryId(long defaultHistoryId) {
    this.defaultHistoryId = defaultHistoryId;
  }

  public List<FileHistoryDto> getHistories() {
    return histories;
  }

  public void setHistories(List<FileHistoryDto> histories) {
    this.histories = histories;
  }

  public Map<String, String> getUserNames() {
    return userNames;
  }

  public void setUserNames(Map<String, String> userNames) {
    this.userNames = userNames;
  }
}
