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

import net.opentsdb.horizon.fs.store.FolderStore;
import net.opentsdb.horizon.converter.ContentConverter;
import net.opentsdb.horizon.model.Content;
import net.opentsdb.horizon.model.ContentHistory;
import net.opentsdb.horizon.model.Snapshot;
import net.opentsdb.horizon.store.AlertStore;
import net.opentsdb.horizon.store.SnapshotStore;
import net.opentsdb.horizon.view.SnapshotView;
import net.opentsdb.horizon.view.SourceType;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Tested;
import mockit.Verifications;
import net.opentsdb.horizon.util.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.opentsdb.horizon.util.Utils.deSerialize;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class SnapshotServiceTest {

  private static MessageDigest digest;
  private static ContentConverter contentConverter;

  @Tested private SnapshotService service;
  @Injectable private SnapshotStore store;
  @Injectable private ContentService contentService;
  @Injectable private FolderStore folderStore;
  @Injectable private AlertStore alertStore;
  @Injectable private ActivityJobScheduler activityJobScheduler;

  @Injectable private Connection rwConnection;
  @Injectable private Connection roConnection;

  @BeforeAll
  private static void beforeAll() throws NoSuchAlgorithmException {
    digest = MessageDigest.getInstance("SHA-256");
    contentConverter = new ContentConverter(digest);
  }

  @BeforeEach
  public void setUp() throws SQLException {
    new Expectations() {
      {
        new Expectations(store) {
          {
            store.getReadWriteConnection();
            result = rwConnection;
            minTimes = 0;

            store.getReadOnlyConnection();
            result = roConnection;
            minTimes = 0;
          }
        };
      }
    };
  }

  @Test
  public void createSnapshot() throws Exception {

    int dashboardId = 123;
    Object contentView =
        new HashMap<String, String>() {
          {
            put("k1", "v1");
            put("k2", "v2");
          }
        };

    Content expectedContent = contentConverter.viewToModel(contentView);

    SnapshotView view = new SnapshotView();
    view.setName("My First Snapshot");
    view.setSourceType(SourceType.ALERT);
    view.setSourceId(dashboardId);
    view.setContent(contentView);

    String principal = "u1";

    new Expectations(contentService) {
      {
        contentService.viewToModel(contentView);
        result = expectedContent;
      }
    };

    service.create(view, principal);

    new Verifications() {
      {
        Snapshot model;

        store.create(rwConnection, model = withCapture());
        times = 1;

        assertSnapshotEquals(view, model);

        expectedContent.setCreatedBy(model.getCreatedBy());
        expectedContent.setCreatedTime(model.getCreatedTime());

        Content actualContent;
        contentService.createContent(rwConnection, actualContent = withCapture());
        times = 1;

        assertContentEquals(expectedContent, actualContent);

        ContentHistory actualHistory;
        contentService.createContentHistory(rwConnection, actualHistory = withCapture());
        times = 1;

        Assertions.assertEquals(SourceType.SNAPSHOT.id, actualHistory.getContentType());
        assertEquals(model.getId(), actualHistory.getEntityId());
        assertArrayEquals(model.getContentId(), actualHistory.getContentId());
        assertEquals(model.getCreatedBy(), actualHistory.getCreatedBy());
        assertEquals(model.getCreatedTime(), actualHistory.getCreatedTime());

        rwConnection.close();
        times = 1;
      }
    };
  }

  @Test
  void getSnapshotById() throws SQLException, IOException {
    Object content = createContent();
    byte[] serialized = Utils.serialize(content).getBytes();
    byte[] contentId = digest.digest(serialized);
    byte[] contentModel = Utils.compress(serialized);
    long id = 123;
    String userId = "user1";
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    Snapshot expected = buildSnapshot("s1", SourceType.DASHBOARD, 456, contentId, userId, timestamp);
    expected.setContent(contentModel);

    new Expectations(store) {
      {
        store.getSnapshotAndContentById(id, roConnection);
        result = expected;
      }
    };

    SnapshotView actual = service.getById(id, userId);
    assertSnapshotEquals(expected, actual);

    new Verifications() {
      {
        activityJobScheduler.addActivity(userId, SourceType.SNAPSHOT.id, id);
        times = 1;

        roConnection.close();
        times = 1;
      }
    };
  }

  @Test
  void doesNotRecordActivityForInvalidSnapshotId() throws SQLException {
    long invalidId = 213;
    String userId = "user1";
    new Expectations() {
      {
        store.getSnapshotAndContentById(invalidId, roConnection);
        result = null;
        times = 1;
      }
    };

    try {
      service.getById(invalidId, userId);
      fail("Should not respond for a invalid id");
    } catch (NotFoundException expected) {
      assertEquals(
          "Snapshot not found with id: " + invalidId,
          expected.getResponse().getEntity().toString());
    }

    new Verifications() {
      {
        activityJobScheduler.addActivity(anyString, anyByte, anyLong);
        times = 0;

        roConnection.close();
        times = 1;
      }
    };
  }

  @Test
  void getRecentlyVisitedSnapshots() throws SQLException, IOException {
    Object content = createContent();
    byte[] serialized = Utils.serialize(content).getBytes();
    byte[] contentId = digest.digest(serialized);
    byte[] contentModel = Utils.compress(serialized);
    long id = 123;
    String userId = "user1";
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    int limit = 10;
    Snapshot snapshot1 = buildSnapshot("s1", SourceType.DASHBOARD, 456, contentId, userId, timestamp);
    Snapshot snapshot2 = buildSnapshot("s2", SourceType.DASHBOARD, 456, contentId, userId, timestamp);

    List<Snapshot> expectedList = Arrays.asList(snapshot1, snapshot2);

    new Expectations(store) {
      {
        store.getRecentlyVisited(roConnection, userId, limit);
        result = expectedList;
        times = 1;
      }
    };

    List<SnapshotView> actualList = service.getRecentlyVisited(userId, limit);
    assertEquals(2, actualList.size());
    assertSnapshotEquals(snapshot1, actualList.get(0));
    assertSnapshotEquals(snapshot2, actualList.get(1));
  }

  private static void assertContentEquals(Content expected, Content actual) {
    assertArrayEquals(expected.getSha2(), actual.getSha2());
    assertArrayEquals(expected.getData(), actual.getData());
    assertEquals(expected.getCreatedBy(), actual.getCreatedBy());
    assertEquals(expected.getCreatedTime(), actual.getCreatedTime());
  }

  private static void assertContentHistoryEquals(ContentHistory expected, ContentHistory actual) {
    assertEquals(expected.getContentType(), actual.getContentType());
    assertEquals(expected.getEntityId(), actual.getEntityId());
    assertArrayEquals(expected.getContentId(), actual.getContentId());
    assertEquals(expected.getCreatedBy(), actual.getCreatedBy());
    assertEquals(expected.getCreatedTime(), actual.getCreatedTime());
  }

  private static void assertSnapshotEquals(SnapshotView view, Snapshot model) throws IOException {
    assertEquals(view.getId(), model.getId());
    assertEquals(view.getName(), model.getName());
    assertEquals(view.getSourceType().id, model.getSourceType());
    assertEquals(view.getSourceId(), model.getSourceId());
    assertArrayEquals(digest.digest(Utils.serialize(view.getContent()).getBytes()), model.getContentId());
  }

  private static void assertSnapshotEquals(Snapshot expected, SnapshotView actual)
      throws IOException {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getName(), actual.getName());
    assertEquals(expected.getSourceType(), actual.getSourceType().id);
    assertEquals(expected.getSourceId(), actual.getSourceId());
    Object expectedContentModel = expected.getContent();
    if (expectedContentModel != null) {
      byte[] decompressed = Utils.decompress((byte[]) expectedContentModel);
      Object expectedContent = Utils.deSerialize(decompressed, Object.class);
      assertEquals(expectedContent, actual.getContent());
    }
  }

  private static Snapshot buildSnapshot(
      String name,
      SourceType sourceType,
      long sourceId,
      byte[] contentId,
      String userId,
      Timestamp timestamp) {
    Snapshot snapshot = new Snapshot();
    snapshot.setName(name);
    snapshot.setSourceType(sourceType.id);
    snapshot.setSourceId(sourceId);
    snapshot.setContentId(contentId);
    snapshot.setCreatedBy(userId);
    snapshot.setCreatedTime(timestamp);
    snapshot.setUpdatedBy(userId);
    snapshot.setUpdatedTime(timestamp);
    return snapshot;
  }

  private Object createContent() {
    Map<String, String> map = new HashMap<>();
    map.put("k1", "v1");
    map.put("k2", "v2");
    return map;
  }
}
