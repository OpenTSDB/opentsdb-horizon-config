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

package net.opentsdb.horizon.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Header;
import com.jayway.restassured.response.Response;
import net.opentsdb.horizon.fs.model.Content;
import net.opentsdb.horizon.fs.model.File;
import net.opentsdb.horizon.fs.model.Folder;
import net.opentsdb.horizon.model.Alert;
import net.opentsdb.horizon.model.AlertType;
import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.model.Activity;
import net.opentsdb.horizon.model.ContentHistory;
import net.opentsdb.horizon.model.Snapshot;
import net.opentsdb.horizon.view.SnapshotView;
import net.opentsdb.horizon.view.SourceType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.jayway.restassured.RestAssured.given;
import static net.opentsdb.horizon.integration.AlertIT.buildDefinition;
import static net.opentsdb.horizon.util.Utils.compress;
import static net.opentsdb.horizon.util.Utils.serialize;
import static net.opentsdb.horizon.util.Utils.slugify;
import static net.opentsdb.horizon.view.SourceType.ALERT;
import static net.opentsdb.horizon.view.SourceType.DASHBOARD;
import static net.opentsdb.horizon.view.SourceType.SNAPSHOT;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class SnapshotIT extends BaseIT {

  private Folder userHomeFolder;
  private File sourceDashboard;
  private Namespace namespace;
  private Alert sourceAlert;
  private Snapshot sourceSnapshot;

  @Override
  protected String getUri() {
    return "snapshot";
  }

  @BeforeEach
  public void beforeMethod() {
    dbUtil.clearTable("activity");
    dbUtil.clearTable("content_history");
    dbUtil.execute("delete from snapshot where id != ?", sourceSnapshot.getId());
    dbUtil.execute("delete from content where sha2 != ?", sourceDashboard.getContentid());
  }

  @BeforeAll
  public void beforeAll() throws IOException {
    userHomeFolder = createUserHomeFolder(regularMember, timestamp);
    long homeId = dbUtil.insert(userHomeFolder);
    userHomeFolder.setId(homeId);

    Map<String, String> contentView = createContent();
    contentView.put("dashboardKey", "dashboardValue");

    Content content = createContent(contentView, timestamp, regularMember);
    sourceDashboard = createDashboard("Dashboard 1", content.getSha2(), timestamp, userHomeFolder);
    dbUtil.insert(content);
    long dashboardId = dbUtil.insert(sourceDashboard);
    sourceDashboard.setId(dashboardId);

    namespace = createNamespace("namesapce1", "test_track", regularMember, timestamp);
    int namespaceId = dbUtil.insert(namespace);
    namespace.setId(namespaceId);
    dbUtil.insertNamespaceMember(namespaceId, regularMember);

    Map<String, Object> definition = AlertIT.buildDefinition();
    List<String> labels = Arrays.asList("integration test", "test");

    sourceAlert =
        AlertIT.buildAlert(
            "test alert",
            AlertType.simple,
            true,
            false,
            namespaceId,
            definition,
            labels,
            regularMember,
            timestamp);
    long id = insertAlert(sourceAlert);
    sourceAlert.setId(id);

    sourceSnapshot =
        buildSnapshot(
            "Source Snapshot",
            DASHBOARD,
            sourceDashboard.getId(),
            content.getSha2(),
            regularMember,
            timestamp);
    dbUtil.insert(sourceSnapshot);
  }

  private Stream<Arguments> buildSnapshots() {
    Object content = createContent();
    SnapshotView view1 =
        buildSnapshotView("Snapshot a Dashboard", DASHBOARD, sourceDashboard.getId(), content);
    SnapshotView view2 =
        buildSnapshotView(
            "  Snapshot a Dashboard    ", DASHBOARD, sourceDashboard.getId(), content);
    SnapshotView view3 = buildSnapshotView("Snapshot a Alert", ALERT, sourceAlert.getId(), content);
    SnapshotView view4 =
        buildSnapshotView("Snapshot a Snapshot", SNAPSHOT, sourceSnapshot.getId(), content);
    SnapshotView view5 = buildSnapshotView("Snapshot without source", null, 0, content);

    return Stream.of(
        arguments(view1, "of a " + DASHBOARD.name()),
        arguments(view2, "trims the name"),
        arguments(view3, "of a " + ALERT.name()),
        arguments(view4, "of a " + SNAPSHOT.name()),
        arguments(view5, "of a " + "adhoc query"));
  }

  @ParameterizedTest(name = "[{index}] {1}")
  @MethodSource("buildSnapshots")
  public void createSnapshot(SnapshotView view, String displayName) throws IOException {

    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(view)
            .post(endPoint);
    assertEquals(201, response.getStatusCode(), response.getBody().asString());

    SnapshotView fromServer = response.as(SnapshotView.class);
    assertSnapshotEquals(view, fromServer, regularMember, timestamp);
    assertNull(fromServer.getContent());

    Snapshot fromDB = dbUtil.getSnapshot(fromServer.getId());
    fromServer.setContent(view.getContent());
    assertSnapshotEquals(fromServer, fromDB);

    Content content = dbUtil.getContentById(fromDB.getContentId());
    assertContentEquals(
        view.getContent(),
        fromDB.getContentId(),
        fromDB.getCreatedBy(),
        fromDB.getCreatedTime(),
        content);

    List<ContentHistory> historyList = dbUtil.getContentHistory(SNAPSHOT.id, fromDB.getId());
    assertEquals(1, historyList.size());
    assertContentHistoryEquals(
        fromDB, fromDB.getCreatedBy(), fromDB.getCreatedTime(), historyList.get(0));
  }

  private Stream<Arguments> buildInvalidSnapshots() {
    Object content = createContent();

    SnapshotView view1 = buildSnapshotView(null, DASHBOARD, sourceDashboard.getId(), content);
    ObjectNode jsonNode1 = OBJECT_MAPPER.convertValue(view1, ObjectNode.class);

    SnapshotView view2 = buildSnapshotView("", DASHBOARD, sourceDashboard.getId(), content);
    ObjectNode jsonNode2 = OBJECT_MAPPER.convertValue(view2, ObjectNode.class);

    SnapshotView view3 = buildSnapshotView("  ", DASHBOARD, sourceDashboard.getId(), content);
    ObjectNode jsonNode3 = OBJECT_MAPPER.convertValue(view3, ObjectNode.class);

    SnapshotView view4 =
        buildSnapshotView("Test snapshot", DASHBOARD, sourceDashboard.getId() + 10, content);
    ObjectNode jsonNode4 = OBJECT_MAPPER.convertValue(view4, ObjectNode.class);

    SnapshotView view5 =
        buildSnapshotView("Test snapshot", DASHBOARD, sourceDashboard.getId(), content);
    ObjectNode jsonNode5 = OBJECT_MAPPER.convertValue(view5, ObjectNode.class);
    jsonNode5.set("sourceType", TextNode.valueOf("INVALID"));

    SnapshotView view6 =
        buildSnapshotView("Test snapshot", DASHBOARD, sourceDashboard.getId(), null);
    ObjectNode jsonNode6 = OBJECT_MAPPER.convertValue(view6, ObjectNode.class);

    return Stream.of(
        arguments(jsonNode1, 400, "Snapshot name empty", true, "with null name"),
        arguments(jsonNode2, 400, "Snapshot name empty", true, "with empty name"),
        arguments(jsonNode3, 400, "Snapshot name empty", true, "with white space name"),
        arguments(jsonNode4, 400, "Source not found", true, "with white invalid sourceId"),
        arguments(jsonNode5, 400, "Source not found", false, "with white invalid sourceType"),
        arguments(jsonNode6, 400, "Content not found", true, "without content"));
  }

  @ParameterizedTest(name = "[{index}] {4}")
  @MethodSource("buildInvalidSnapshots")
  void cannotCreateSnapshot(
      JsonNode view,
      int expectedHttpCode,
      String expectedMessage,
      boolean assertMessage,
      String displayName) {
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(view)
            .post(endPoint);
    String message = response.getBody().asString();
    assertEquals(expectedHttpCode, response.getStatusCode());
    if (assertMessage) {
      assertEquals(expectedMessage, message);
    }
    assertEquals(0, dbUtil.getRecordCount("snapshot", "id != " + sourceSnapshot.getId()));
    assertEquals(1, dbUtil.getRecordCount("content")); // dashboard content only
    assertEquals(0, dbUtil.getRecordCount("content_history"));
  }

  @Test
  void sameContentIsSharedAcrossSnapshots() throws IOException {

    assertEquals(1, dbUtil.getRecordCount("content")); // dashboard's content

    Object content = createContent();
    SnapshotView view1 =
        buildSnapshotView("Snapshot a Dashboard", DASHBOARD, sourceDashboard.getId(), content);
    SnapshotView view2 = buildSnapshotView("Snapshot a Alert", ALERT, sourceAlert.getId(), content);

    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(view1)
            .post(endPoint);
    assertEquals(201, response.getStatusCode(), response.getBody().asString());
    SnapshotView fromServer1 = response.as(SnapshotView.class);
    assertSnapshotEquals(view1, fromServer1, regularMember, timestamp);
    assertNull(fromServer1.getContent());

    byte[] expectedContentId = sha256.digest(serialize(content).getBytes());

    Snapshot fromDB1 = dbUtil.getSnapshot(fromServer1.getId());
    fromServer1.setContent(view1.getContent());
    assertSnapshotEquals(fromServer1, fromDB1);
    Content contentFromDB1 = dbUtil.getContentById(fromDB1.getContentId());
    assertContentEquals(
        view1.getContent(),
        fromDB1.getContentId(),
        fromDB1.getCreatedBy(),
        fromDB1.getCreatedTime(),
        contentFromDB1);
    List<ContentHistory> historyList1 = dbUtil.getContentHistory(SNAPSHOT.id, fromDB1.getId());
    assertEquals(1, historyList1.size());
    assertContentHistoryEquals(
        fromDB1, fromDB1.getCreatedBy(), fromDB1.getCreatedTime(), historyList1.get(0));

    assertArrayEquals(expectedContentId, fromDB1.getContentId());
    assertEquals(2, dbUtil.getRecordCount("content")); // snapshot content got added

    response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(view2)
            .post(endPoint);
    assertEquals(201, response.getStatusCode());
    SnapshotView fromServer2 = response.as(SnapshotView.class);
    assertSnapshotEquals(view2, fromServer2, regularMember, timestamp);
    assertNull(fromServer2.getContent());

    Snapshot fromDB2 = dbUtil.getSnapshot(fromServer2.getId());
    fromServer2.setContent(view2.getContent());
    assertSnapshotEquals(fromServer2, fromDB2);
    Content contentFromDB2 = dbUtil.getContentById(fromDB2.getContentId());
    assertContentEquals(
        view2.getContent(),
        fromDB1.getContentId(),
        fromDB1.getCreatedBy(),
        fromDB1.getCreatedTime(),
        contentFromDB2); // refers to the content of 1st snapshot
    List<ContentHistory> historyList2 = dbUtil.getContentHistory(SNAPSHOT.id, fromDB2.getId());
    assertEquals(1, historyList2.size());
    assertContentHistoryEquals(
        fromDB2, fromDB2.getCreatedBy(), fromDB2.getCreatedTime(), historyList2.get(0));

    assertArrayEquals(expectedContentId, fromDB2.getContentId());

    assertEquals(2, dbUtil.getRecordCount("snapshot", "id != " + sourceSnapshot.getId()));
    assertEquals(2, dbUtil.getRecordCount("content_history"));
    assertEquals(2, dbUtil.getRecordCount("content")); // snapshot content is reused
  }

  private Stream<Arguments> buildSnapshotsToUpdate() throws IOException {
    Map<String, String> content = createContent();
    net.opentsdb.horizon.model.Content originalContent =
        buildContent(content, regularMember, timestamp);
    Snapshot originalSnapshot =
        buildSnapshot(
            "Test Snapshot",
            DASHBOARD,
            sourceDashboard.getId(),
            originalContent.getSha2(),
            regularMember,
            timestamp);

    content.put("k3", "v3");
    net.opentsdb.horizon.model.Content newContent =
        buildContent(content, regularMember, timestamp);
    String newName = originalSnapshot.getName() + " updated";
    SnapshotView toUpdate1 =
        buildSnapshotView(
            originalSnapshot.getId(),
            newName,
            ALERT,
            sourceAlert.getId(),
            content,
            originalSnapshot.getCreatedBy(),
            originalSnapshot.getCreatedTime());
    ObjectNode jsonNode1 = OBJECT_MAPPER.convertValue(toUpdate1, ObjectNode.class);

    SnapshotView toUpdate2 =
        buildSnapshotView(
            originalSnapshot.getId(),
            newName,
            SourceType.valueOf(originalSnapshot.getSourceType()),
            originalSnapshot.getSourceId(),
            null,
            originalSnapshot.getCreatedBy(),
            originalSnapshot.getCreatedTime());
    ObjectNode jsonNode2 = OBJECT_MAPPER.convertValue(toUpdate2, ObjectNode.class);
    jsonNode2.remove("sourceType");
    jsonNode2.remove("sourceId");
    jsonNode2.remove("content");
    jsonNode2.remove("createdBy");
    jsonNode2.remove("createdTime");

    SnapshotView toUpdate3 =
        buildSnapshotView(
            originalSnapshot.getId(),
            originalSnapshot.getName(),
            SourceType.valueOf(originalSnapshot.getSourceType()),
            originalSnapshot.getSourceId(),
            content,
            originalSnapshot.getCreatedBy(),
            originalSnapshot.getCreatedTime());
    ObjectNode jsonNode3 = OBJECT_MAPPER.convertValue(toUpdate3, ObjectNode.class);
    jsonNode3.remove("name");
    jsonNode3.remove("sourceType");
    jsonNode3.remove("sourceId");
    jsonNode3.remove("createdBy");
    jsonNode3.remove("createdTime");

    SnapshotView toUpdate4 =
        buildSnapshotView(
            originalSnapshot.getId(),
            originalSnapshot.getName(),
            ALERT,
            sourceAlert.getId(),
            null,
            originalSnapshot.getCreatedBy(),
            originalSnapshot.getCreatedTime());
    ObjectNode jsonNode4 = OBJECT_MAPPER.convertValue(toUpdate4, ObjectNode.class);
    jsonNode4.remove("name");
    jsonNode4.remove("createdBy");
    jsonNode4.remove("createdTime");

    SnapshotView toUpdate5 =
        buildSnapshotView(
            originalSnapshot.getId(),
            "  " + originalSnapshot.getName() + " updated      ",
            SourceType.valueOf(originalSnapshot.getSourceType()),
            originalSnapshot.getSourceId(),
            null,
            originalSnapshot.getCreatedBy(),
            originalSnapshot.getCreatedTime());
    ObjectNode jsonNode5 = OBJECT_MAPPER.convertValue(toUpdate5, ObjectNode.class);
    jsonNode5.remove("sourceType");
    jsonNode5.remove("sourceId");
    jsonNode5.remove("content");
    jsonNode5.remove("createdBy");
    jsonNode5.remove("createdTime");

    return Stream.of(
        arguments(
            originalContent,
            newContent,
            originalSnapshot,
            toUpdate1,
            jsonNode1,
            "all allowed fields"),
        arguments(
            originalContent, originalContent, originalSnapshot, toUpdate2, jsonNode2, "name only"),
        arguments(
            originalContent, newContent, originalSnapshot, toUpdate3, jsonNode3, "content only"),
        arguments(
            originalContent,
            originalContent,
            originalSnapshot,
            toUpdate4,
            jsonNode4,
            "sourceType and sourceId only"),
        arguments(
            originalContent,
            originalContent,
            originalSnapshot,
            toUpdate5,
            jsonNode5,
            "trims the name"));
  }

  @ParameterizedTest(name = "[{index}] {5}")
  @MethodSource("buildSnapshotsToUpdate")
  void updateSnapshot(
      net.opentsdb.horizon.model.Content originalContent,
      net.opentsdb.horizon.model.Content expectedContent,
      Snapshot originalSnapshot,
      SnapshotView expected,
      ObjectNode payload,
      String displayName)
      throws IOException {

    dbUtil.insert(originalContent);
    dbUtil.insert(originalSnapshot);
    ContentHistory contentHistoryModel =
        buildContentHistory(
            SNAPSHOT,
            originalSnapshot.getId(),
            originalContent.getSha2(),
            regularMember,
            timestamp);
    dbUtil.insert(contentHistoryModel);

    expected.setId(originalSnapshot.getId());
    payload.set("id", LongNode.valueOf(originalSnapshot.getId()));

    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(payload)
            .put(endPoint)
            .andReturn();
    assertEquals(200, response.getStatusCode(), response.getBody().asString());
    SnapshotView fromServer = response.as(SnapshotView.class);
    assertSnapshotEquals(expected, fromServer, regularMember, timestamp);
    assertNull(fromServer.getContent());

    Snapshot fromDB = dbUtil.getSnapshot(fromServer.getId());
    assertSnapshotEquals(fromServer, fromDB);

    Content contentFromDB = dbUtil.getContentById(fromDB.getContentId());
    List<ContentHistory> historyList = dbUtil.getContentHistory(SNAPSHOT.id, fromDB.getId());
    boolean contentUpdated = !Arrays.equals(originalContent.getSha2(), expectedContent.getSha2());

    if (contentUpdated) {
      assertContentEquals(
          expectedContent,
          fromDB.getContentId(),
          fromDB.getUpdatedBy(),
          fromDB.getUpdatedTime(),
          contentFromDB);
      assertEquals(2, historyList.size());
      assertContentHistoryEquals(
          fromDB, fromDB.getUpdatedBy(), fromDB.getUpdatedTime(), historyList.get(1));
    } else {
      assertContentEquals(
          expectedContent,
          fromDB.getContentId(),
          fromDB.getCreatedBy(),
          fromDB.getCreatedTime(),
          contentFromDB);
      assertEquals(1, historyList.size());
      assertContentHistoryEquals(
          fromDB, fromDB.getCreatedBy(), fromDB.getCreatedTime(), historyList.get(0));
    }
  }

  private Stream<Arguments> buildInvalidSnapshotsToUpdate() throws IOException {
    Map<String, String> content = createContent();
    String createdBy = BaseIT.regularMember;
    net.opentsdb.horizon.model.Content originalContent =
        buildContent(content, createdBy, timestamp);
    Snapshot originalSnapshot =
        buildSnapshot(
            "Test Snapshot",
            DASHBOARD,
            sourceDashboard.getId(),
            originalContent.getSha2(),
            createdBy,
            timestamp);

    String newName = originalSnapshot.getName() + " updated";
    SnapshotView toUpdate1 =
        buildSnapshotView(
            originalSnapshot.getId(),
            newName,
            ALERT,
            sourceAlert.getId(),
            content,
            originalSnapshot.getCreatedBy(),
            originalSnapshot.getCreatedTime());
    ObjectNode jsonNode1 = OBJECT_MAPPER.convertValue(toUpdate1, ObjectNode.class);

    SnapshotView toUpdate2 =
        buildSnapshotView(
            originalSnapshot.getId(),
            "",
            SourceType.valueOf(originalSnapshot.getSourceType()),
            originalSnapshot.getSourceId(),
            null,
            originalSnapshot.getCreatedBy(),
            originalSnapshot.getCreatedTime());
    ObjectNode jsonNode2 = OBJECT_MAPPER.convertValue(toUpdate2, ObjectNode.class);
    jsonNode2.remove("sourceType");
    jsonNode2.remove("sourceId");
    jsonNode2.remove("content");
    jsonNode2.remove("createdBy");
    jsonNode2.remove("createdTime");

    SnapshotView toUpdate3 =
        buildSnapshotView(
            originalSnapshot.getId(),
            "  ",
            SourceType.valueOf(originalSnapshot.getSourceType()),
            originalSnapshot.getSourceId(),
            null,
            originalSnapshot.getCreatedBy(),
            originalSnapshot.getCreatedTime());
    ObjectNode jsonNode3 = OBJECT_MAPPER.convertValue(toUpdate3, ObjectNode.class);
    jsonNode3.remove("sourceType");
    jsonNode3.remove("sourceId");
    jsonNode3.remove("content");
    jsonNode3.remove("createdBy");
    jsonNode3.remove("createdTime");

    SnapshotView toUpdate4 =
        buildSnapshotView(
            originalSnapshot.getId(),
            originalSnapshot.getName(),
            ALERT,
            sourceAlert.getId(),
            null,
            originalSnapshot.getCreatedBy(),
            originalSnapshot.getCreatedTime());
    ObjectNode jsonNode4 = OBJECT_MAPPER.convertValue(toUpdate4, ObjectNode.class);
    jsonNode4.set("sourceType", TextNode.valueOf("INVALID"));
    jsonNode4.remove("content");
    jsonNode4.remove("createdBy");
    jsonNode4.remove("createdTime");

    SnapshotView toUpdate5 =
        buildSnapshotView(
            originalSnapshot.getId(),
            originalSnapshot.getName(),
            ALERT,
            sourceAlert.getId() + 10,
            null,
            originalSnapshot.getCreatedBy(),
            originalSnapshot.getCreatedTime());
    ObjectNode jsonNode5 = OBJECT_MAPPER.convertValue(toUpdate5, ObjectNode.class);
    jsonNode5.remove("content");
    jsonNode5.remove("createdBy");
    jsonNode5.remove("createdTime");

    SnapshotView toUpdate6 =
        buildSnapshotView(
            originalSnapshot.getId(),
            newName,
            SNAPSHOT,
            sourceSnapshot.getId(),
            content,
            originalSnapshot.getCreatedBy(),
            originalSnapshot.getCreatedTime());
    ObjectNode jsonNode6 = OBJECT_MAPPER.convertValue(toUpdate6, ObjectNode.class);

    return Stream.of(
        arguments(
            originalContent,
            originalSnapshot,
            jsonNode1,
            createdBy,
            false,
            404,
            "Snapshot not found with id: ",
            false,
            true,
            "with invalid id"),
        arguments(
            originalContent,
            originalSnapshot,
            jsonNode2,
            createdBy,
            true,
            400,
            "Snapshot name empty",
            false,
            true,
            "with empty name"),
        arguments(
            originalContent,
            originalSnapshot,
            jsonNode3,
            createdBy,
            true,
            400,
            "Snapshot name empty",
            false,
            true,
            "with white space name"),
        arguments(
            originalContent,
            originalSnapshot,
            jsonNode4,
            createdBy,
            true,
            400,
            "Source not found",
            false,
            false,
            "with invalid sourceType"),
        arguments(
            originalContent,
            originalSnapshot,
            jsonNode5,
            createdBy,
            true,
            400,
            "Source not found",
            false,
            true,
            "with invalid sourceId"),
        arguments(
            originalContent,
            originalSnapshot,
            jsonNode1,
            unauthorizedMember,
            true,
            403,
            "Access denied",
            false,
            true,
            "by unauthorized user"),
        arguments(
            originalContent,
            originalSnapshot,
            jsonNode6,
            createdBy,
            true,
            400,
            "Source pointing to the snapshot itself",
            true,
            true,
            "with own id as sourceId"));
  }

  @ParameterizedTest(name = "[{index}] {9}")
  @MethodSource("buildInvalidSnapshotsToUpdate")
  void cannotUpdateSnapshot(
      net.opentsdb.horizon.model.Content originalContent,
      Snapshot originalSnapshot,
      ObjectNode payload,
      String updatedBy,
      boolean useValidId,
      int expectedCode,
      String expectedMessage,
      boolean useOwnIdAsSourceId,
      boolean assertMessage,
      String displayName) {

    dbUtil.insert(originalContent);
    dbUtil.insert(originalSnapshot);
    ContentHistory contentHistory =
        buildContentHistory(
            SNAPSHOT,
            originalSnapshot.getId(),
            originalContent.getSha2(),
            originalSnapshot.getCreatedBy(),
            timestamp);
    dbUtil.insert(contentHistory);

    if (useValidId) {
      payload.set("id", LongNode.valueOf(originalSnapshot.getId()));
    } else {
      long invalidId = originalSnapshot.getId() + 10;
      payload.set("id", LongNode.valueOf(invalidId));
      expectedMessage = expectedMessage + invalidId;
    }
    if (useOwnIdAsSourceId) {
      payload.set("sourceId", LongNode.valueOf(originalSnapshot.getId()));
    }

    String oktaCookie = createOktaCookie(updatedBy);
    Header cookieHeader = new Header("Cookie", oktaCookie);

    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(payload)
            .put(endPoint)
            .andReturn();
    String actualMessage = response.getBody().asString();
    assertEquals(expectedCode, response.getStatusCode(), actualMessage);
    if (assertMessage) {
      assertEquals(expectedMessage, actualMessage);
    }
  }

  @Test
  void getSnapshotById() throws IOException {
    Object content = createContent();
    net.opentsdb.horizon.model.Content contentModel =
        buildContent(content, regularMember, timestamp);
    dbUtil.insert(contentModel);
    Snapshot model =
        buildSnapshot(
            "Test Snapshot",
            DASHBOARD,
            sourceDashboard.getId(),
            contentModel.getSha2(),
            regularMember,
            timestamp);
    dbUtil.insert(model);
    ContentHistory contentHistoryModel =
        buildContentHistory(
            SNAPSHOT, model.getId(), contentModel.getSha2(), regularMember, timestamp);
    dbUtil.insert(contentHistoryModel);

    String url = endPoint + "/" + model.getId();

    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(url)
            .andReturn();
    assertEquals(200, response.getStatusCode(), response.getBody().asString());
    final SnapshotView fromServer = response.getBody().as(SnapshotView.class);
    assertSnapshotEquals(model, fromServer);
    assertContentEquals(contentModel, fromServer.getContent());
  }

  @Test
  void getSnapshotByInvalidId() {
    long invalidId = -10;
    String url = endPoint + "/" + invalidId;

    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(url)
            .andReturn();
    String message = response.getBody().asString();
    assertEquals(404, response.getStatusCode(), message);
    assertEquals("Snapshot not found with id: " + invalidId, message);
  }

  @Test
  void recordLastVisitedTimeUponReadingById() throws IOException, InterruptedException {
    Object content = createContent();
    net.opentsdb.horizon.model.Content contentModel =
        buildContent(content, regularMember, timestamp);
    dbUtil.insert(contentModel);
    Snapshot model =
        buildSnapshot(
            "Test Snapshot", DASHBOARD, 123, contentModel.getSha2(), regularMember, timestamp);
    dbUtil.insert(model);

    Timestamp ts1 = new Timestamp(System.currentTimeMillis());

    String url = endPoint + "/" + model.getId();

    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(url)
            .andReturn();
    assertEquals(200, response.getStatusCode(), response.getBody().asString());

    Thread.sleep(2000); // wait for the snapshot activity updated in async.

    Activity activity1 = dbUtil.getActivity(regularMember, SNAPSHOT.id, model.getId());
    assertEquals(regularMember, activity1.getUserId());
    assertEquals(SNAPSHOT.id, activity1.getEntityType());
    assertEquals(model.getId(), activity1.getEntityId());
    assertTrue(ts1.getTime() <= activity1.getTimestamp().getTime());

    // verify that reading again, updates the last visited time of the same record.
    response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(url)
            .andReturn();
    assertEquals(200, response.getStatusCode(), response.getBody().asString());

    Thread.sleep(1000); // wait for the snapshot activity updated in async.

    Activity activity2 = dbUtil.getActivity(regularMember, SNAPSHOT.id, model.getId());
    assertEquals(regularMember, activity2.getUserId());
    assertEquals(SNAPSHOT.id, activity2.getEntityType());
    assertEquals(model.getId(), activity2.getEntityId());
    assertTrue(activity1.getTimestamp().getTime() <= activity2.getTimestamp().getTime());
  }

  @Test
  void getMyRecentlyVisitedSnapshotsOrderedByLastVisitedTime() {
    Stream<Arguments> snapshots = buildSnapshots();
    List<SnapshotView> views =
        snapshots.map(argument -> (SnapshotView) (argument.get()[0])).collect(Collectors.toList());
    List<Snapshot> models = new ArrayList<>();
    views.forEach(
        view -> {
          view.setCreatedBy(regularMember);
          view.setCreatedTime(timestamp);
          view.setUpdatedBy(regularMember);
          view.setUpdatedTime(timestamp);
          Snapshot model = toModel(view);
          try {
            net.opentsdb.horizon.model.Content content =
                buildContent(view.getContent(), regularMember, timestamp);
            dbUtil.insert(content);

            model.setContentId(content.getSha2());
          } catch (IOException e) {
            fail(e.getMessage());
          }
          dbUtil.insert(model);
          models.add(model);
        });

    List<Activity> activities = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      Snapshot model = models.get(i);
      Timestamp lastVisitedTime = new Timestamp(timestamp.getTime() - i * 10);
      Activity activity = buildActivity(regularMember, SNAPSHOT.id, model.getId(), lastVisitedTime);
      dbUtil.insert(activity);
      activities.add(activity);
    }

    Timestamp lastVisitedTime = new Timestamp(timestamp.getTime() + 1);
    Activity activity =
        buildActivity(unauthorizedMember, SNAPSHOT.id, models.get(3).getId(), lastVisitedTime);
    dbUtil.insert(activity);
    activities.add(activity);

    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(endPoint + "/recent");
    assumeTrue(200 == response.getStatusCode());
    List<SnapshotView> fromServer = Arrays.asList(response.getBody().as(SnapshotView[].class));
    assertEquals(3, fromServer.size());
    for (int i = 0; i < 3; i++) {
      SnapshotView actual = fromServer.get(i);
      assertSnapshotEquals(models.get(i), actual);
      assertEquals(activities.get(i).getTimestamp(), actual.getLastVisitedTime());
    }

    // get recently visited dashboards for a different user
    response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(endPoint + "/recent?userId=" + unauthorizedMember);
    assumeTrue(200 == response.getStatusCode());
    fromServer = Arrays.asList(response.getBody().as(SnapshotView[].class));
    assertEquals(1, fromServer.size());
    SnapshotView actual = fromServer.get(0);
    assertSnapshotEquals(models.get(3), actual);
    assertEquals(activities.get(3).getTimestamp(), actual.getLastVisitedTime());
  }

  private Activity buildActivity(
      String userId, byte entityType, long entityId, Timestamp lastVisitedTime) {
    Activity activity = new Activity();
    activity.setUserId(userId);
    activity.setEntityType(entityType);
    activity.setEntityId(entityId);
    activity.setTimestamp(lastVisitedTime);
    return activity;
  }

  private ContentHistory buildContentHistory(
      SourceType sourceType, long entityId, byte[] contentId, String userId, Timestamp timestamp) {
    ContentHistory model = new ContentHistory();
    model.setContentType(sourceType.id);
    model.setEntityId(entityId);
    model.setContentId(contentId);
    model.setCreatedBy(userId);
    model.setCreatedTime(timestamp);
    return model;
  }

  private net.opentsdb.horizon.model.Content buildContent(
      Object content, String userId, Timestamp timestamp) throws IOException {
    byte[] serialized = serialize(content).getBytes();
    byte[] sha2 = sha256.digest(serialized);
    net.opentsdb.horizon.model.Content model =
        new net.opentsdb.horizon.model.Content(sha2, compress(serialized));
    model.setCreatedBy(userId);
    model.setCreatedTime(timestamp);
    return model;
  }

  private SnapshotView buildSnapshotView(
      long id,
      String name,
      SourceType sourceType,
      long sourceId,
      Object content,
      String createdBy,
      Timestamp createdTime) {
    SnapshotView view = buildSnapshotView(name, sourceType, sourceId, content);
    view.setId(id);
    view.setCreatedBy(createdBy);
    view.setCreatedTime(createdTime);
    return view;
  }

  private SnapshotView buildSnapshotView(
      String name, SourceType sourceType, long sourceId, Object content) {
    SnapshotView view = new SnapshotView();
    view.setName(name);
    view.setSourceType(sourceType);
    view.setSourceId(sourceId);
    view.setContent(content);
    return view;
  }

  private Snapshot buildSnapshot(
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

  private Snapshot toModel(SnapshotView view) {
    Snapshot model = new Snapshot();
    model.setId(view.getId());
    model.setName(view.getName());
    SourceType sourceType = view.getSourceType();
    if (sourceType != null) {
      model.setSourceType(sourceType.id);
    }
    model.setSourceId(view.getSourceId());
    model.setContent(view.getContent());
    model.setCreatedBy(view.getCreatedBy());
    model.setCreatedTime(view.getCreatedTime());
    model.setUpdatedBy(view.getUpdatedBy());
    model.setUpdatedTime(view.getUpdatedTime());
    return model;
  }

  private void assertContentHistoryEquals(
      Snapshot entity, String createdBy, Timestamp createdTime, ContentHistory contentHistory) {
    assertTrue(contentHistory.getId() > 0);
    assertEquals(SNAPSHOT.id, contentHistory.getContentType());
    assertEquals(entity.getId(), contentHistory.getEntityId());
    assertArrayEquals(entity.getContentId(), contentHistory.getContentId());
    assertEquals(createdBy, contentHistory.getCreatedBy());
    assertEquals(createdTime, contentHistory.getCreatedTime());
  }

  private void assertContentEquals(
      Object expectedContent,
      byte[] expectedContentId,
      String expectedCreatedBy,
      Timestamp expectedCreatedTime,
      Content actual)
      throws IOException {
    byte[] serialized = serialize(expectedContent).getBytes();
    byte[] expectedBytes = compress(serialized);

    assertArrayEquals(expectedContentId, actual.getSha2());
    assertArrayEquals(expectedBytes, actual.getData());
    assertEquals(expectedCreatedBy, actual.getCreatedby());
    assertEquals(expectedCreatedTime, actual.getCreatedtime());
  }

  private void assertContentEquals(
      net.opentsdb.horizon.model.Content expectedContent,
      byte[] expectedContentId,
      String expectedCreatedBy,
      Timestamp expectedCreatedTime,
      Content actual) {

    assertArrayEquals(expectedContentId, actual.getSha2());
    assertArrayEquals(expectedContent.getSha2(), actual.getSha2());
    assertArrayEquals(expectedContent.getData(), actual.getData());
    assertEquals(expectedCreatedBy, actual.getCreatedby());
    assertEquals(expectedCreatedTime, actual.getCreatedtime());
  }

  private void assertContentEquals(net.opentsdb.horizon.model.Content expected, Object actual)
      throws IOException {
    assertNotNull(actual);
    byte[] serialized = serialize(actual).getBytes();
    byte[] actualSha2 = sha256.digest(serialized);
    byte[] actualData = compress(serialized);

    assertArrayEquals(expected.getSha2(), actualSha2);
    assertArrayEquals(expected.getData(), actualData);
  }

  private void assertSnapshotEquals(SnapshotView expected, Snapshot actual) throws IOException {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getName(), actual.getName());
    if (expected.getSourceType() == null) {
      assertEquals(0, actual.getSourceType());
    } else {
      assertEquals(expected.getSourceType().id, actual.getSourceType());
    }
    assertEquals(expected.getSourceId(), actual.getSourceId());
    Object content = expected.getContent();
    if (content != null) {
      byte[] serialized = serialize(content).getBytes();
      byte[] sha2 = sha256.digest(serialized);
      assertArrayEquals(sha2, actual.getContentId());
    }
    assertEquals(expected.getCreatedBy(), actual.getCreatedBy());
    assertEquals(expected.getCreatedTime(), actual.getCreatedTime());
    assertEquals(expected.getUpdatedBy(), actual.getUpdatedBy());
    assertEquals(expected.getUpdatedTime(), actual.getUpdatedTime());
  }

  private static void assertSnapshotEquals(
      SnapshotView expected, SnapshotView actual, String userId, Timestamp timestamp) {
    assertEquals(expected.getName().trim(), actual.getName());
    assertEquals(expected.getSourceType(), actual.getSourceType());
    assertEquals(expected.getSourceId(), actual.getSourceId());
    assertEquals("/" + actual.getId() + "/" + slugify(actual.getName()), actual.getPath());
    if (expected.getId() <= 0) { // newly created
      assertTrue(actual.getId() > 0);
      assertEquals(userId, actual.getCreatedBy());
      assertTrue(timestamp.getTime() <= actual.getCreatedTime().getTime());
      assertEquals(userId, actual.getUpdatedBy());
      assertEquals(actual.getCreatedTime(), actual.getUpdatedTime());
    } else {
      assertEquals(expected.getId(), actual.getId());
      assertEquals(expected.getCreatedBy(), actual.getCreatedBy());
      assertEquals(expected.getCreatedTime(), actual.getCreatedTime());
      assertEquals(userId, actual.getUpdatedBy());
      assertTrue(expected.getCreatedTime().getTime() <= actual.getUpdatedTime().getTime());
    }
  }

  private void assertSnapshotEquals(Snapshot expected, SnapshotView actual) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getName(), actual.getName());
    if (actual.getSourceType() != null) {
      assertEquals(expected.getSourceType(), actual.getSourceType().id);
    }
    assertEquals(expected.getSourceId(), actual.getSourceId());
    assertEquals("/" + actual.getId() + "/" + slugify(actual.getName()), actual.getPath());
    assertEquals(expected.getCreatedBy(), actual.getCreatedBy());
    assertEquals(expected.getCreatedTime(), actual.getCreatedTime());
    assertEquals(expected.getUpdatedBy(), actual.getUpdatedBy());
    assertEquals(expected.getUpdatedTime(), actual.getUpdatedTime());
  }

  private Map<String, String> createContent() {
    Map<String, String> map = new HashMap<>();
    map.put("k1", "v1");
    map.put("k2", "v2");
    return map;
  }
}
