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

import net.opentsdb.horizon.converter.ContentConverter;
import net.opentsdb.horizon.model.Content;
import net.opentsdb.horizon.model.ContentHistory;
import net.opentsdb.horizon.store.ContentStore;

import java.io.IOException;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class ContentService extends BaseService<Object, Content, ContentConverter> {

  private final ContentStore store;

  public ContentService(final MessageDigest digest, final ContentStore store) {
    super(new ContentConverter(digest), store);
    this.store = store;
  }

  public void createContent(Connection connection, Content content) throws SQLException {
    store.createContent(connection, content);
  }

  public void createContentHistory(Connection connection, ContentHistory history)
      throws SQLException {
    store.createContentHistory(connection, history);
  }

  public List<ContentHistory> getContentHistory(
      final Connection connection, final byte contentType, final long entityId)
      throws SQLException {
    return store.getContentHistory(connection, contentType, entityId);
  }

  public Content viewToModel(Object view) throws IOException {
    return converter.viewToModel(view);
  }
}
