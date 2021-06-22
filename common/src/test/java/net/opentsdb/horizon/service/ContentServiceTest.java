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

import net.opentsdb.horizon.model.Content;
import net.opentsdb.horizon.model.ContentHistory;
import net.opentsdb.horizon.store.ContentStore;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Tested;
import mockit.Verifications;
import net.opentsdb.horizon.util.Utils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

public class ContentServiceTest {

  @Tested private ContentService service;
  @Injectable private MessageDigest digest;
  @Injectable private ContentStore store;
  @Injectable private Connection connection;

  @Test
  public void viewToModel() throws IOException {
    Object view =
        new HashMap() {
          {
            put("k1", "v1");
            put("k2", "v2");
          }
        };

    byte[] compressedData = Utils.compress(Utils.serialize(view).getBytes());
    byte[] sha2 = new byte[1];

    new Expectations(digest) {
      {
        digest.digest(withInstanceOf(byte[].class));
        result = sha2;
      }
    };

    Content model = service.viewToModel(view);
    assertArrayEquals(sha2, model.getSha2());
    assertArrayEquals(compressedData, model.getData());
  }

  @Test
  void createContent() throws SQLException {
    byte[] sha2 = new byte[1];
    byte[] data = new byte[2];
    String userId = "u1";
    Timestamp timestamp = BaseService.now();

    long entityId = 123;
    byte contentType = (byte) 1;

    Content content = new Content(sha2, data);
    content.setCreatedBy(userId);
    content.setCreatedTime(timestamp);

    service.createContent(connection, content);

    new Verifications() {
      {
        Content actual;
        store.createContent(connection, actual = withCapture());
        times = 1;

        assertContent(content, actual);
      }
    };
  }

  @Test
  void createContentHistory() throws SQLException {
    byte[] sha2 = new byte[1];
    String userId = "u1";
    Timestamp timestamp = BaseService.now();

    long entityId = 123;
    byte contentType = (byte) 1;

    ContentHistory history = new ContentHistory();
    history.setContentType(contentType);
    history.setEntityId(entityId);
    history.setContentId(sha2);
    history.setCreatedBy(userId);
    history.setCreatedTime(timestamp);

    service.createContentHistory(connection, history);

    new Verifications() {
      {
        ContentHistory actual;
        store.createContentHistory(connection, actual = withCapture());
        times = 1;

        assertContentHistoryEquals(history, actual);
      }
    };
  }

  @Test
  void getContentHistory() throws SQLException {
    byte contentType = (byte) 1;
    long entityId = 123;
    List<ContentHistory> expected = Arrays.asList(new ContentHistory());

    new Expectations(store) {
      {
        store.getContentHistory(connection, contentType, entityId);
        result = expected;
      }
    };

    List<ContentHistory> actual = service.getContentHistory(connection, contentType, entityId);
    assertIterableEquals(expected, actual);
  }

  public static void assertContentHistoryEquals(ContentHistory expected, ContentHistory actual) {
    assertEquals(expected.getContentType(), actual.getContentType());
    assertEquals(expected.getEntityId(), actual.getEntityId());
    assertArrayEquals(expected.getContentId(), actual.getContentId());
    assertEquals(expected.getCreatedBy(), actual.getCreatedBy());
    assertEquals(expected.getCreatedTime(), actual.getCreatedTime());
  }

  public static void assertContent(Content expected, Content actual) {
    assertArrayEquals(expected.getSha2(), actual.getSha2());
    assertArrayEquals(expected.getData(), actual.getData());
    assertEquals(expected.getCreatedBy(), actual.getCreatedBy());
    assertEquals(expected.getCreatedTime(), actual.getCreatedTime());
  }
}
