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

import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Response;
import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.model.Snooze;
import net.opentsdb.horizon.view.SnoozeView;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.jayway.restassured.RestAssured.given;
import static net.opentsdb.horizon.converter.AlertConverter.NOTIFICATION;
import static net.opentsdb.horizon.converter.SnoozeConverter.ALERTIDS;
import static net.opentsdb.horizon.converter.SnoozeConverter.FILTER;
import static net.opentsdb.horizon.converter.SnoozeConverter.LABELS;
import static net.opentsdb.horizon.converter.SnoozeConverter.REASON;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.EMPTY_MAP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class SnoozeIT extends BaseIT {

  private Timestamp timestamp;
  private Namespace namespace1;
  private Namespace namespace2;

  @Override
  protected String getUri() {
    return "namespace";
  }

  @BeforeAll
  void beforeAll() throws IOException {
    timestamp = new Timestamp(System.currentTimeMillis());
    namespace1 = createNamespace("namesapce1", "test_track", regularMember, timestamp);
    namespace2 = createNamespace("namesapce2", "test_track", regularMember, timestamp);
    int namespaceId1 = dbUtil.insert(namespace1);
    int namespaceId2 = dbUtil.insert(namespace2);
    namespace1.setId(namespaceId1);
    namespace2.setId(namespaceId2);
    dbUtil.insertNamespaceMember(namespaceId1, regularMember);
    dbUtil.insertNamespaceMember(namespaceId2, regularMember);
  }

  @BeforeEach
  void beforeEach() {
    cascadeDeleteSnooze();
  }

  @ParameterizedTest(name = "[{index}] {1}")
  @MethodSource("buildSnooze")
  public void createSnooze(List<SnoozeView> expectedList, String displayName) throws IOException {

    String url = endPoint + "/" + namespace1.getName() + "/snooze";
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(expectedList)
            .post(url);
    assertEquals(201, response.getStatusCode(), () -> response.getBody().asString());
    SnoozeView[] fromServer = response.as(SnoozeView[].class);
    assertEquals(expectedList.size(), fromServer.length);

    for (int i = 0; i < expectedList.size(); i++) {
      assertSnoozeEquals(expectedList.get(i), fromServer[i], namespace1, regularMember, timestamp);

      SnoozeView actual = fromServer[i];
      Snooze fromDB = dbUtil.getSnooze(actual.getId()).get();
      assertSnoozeEquals(actual, fromDB, namespace1);
    }
  }

  @Test
  void getById() throws IOException {
    Map<String, Object> definition =
        new HashMap() {
          {
            put(ALERTIDS, Arrays.asList(1, 2));
          }
        };
    Timestamp plus1Hr = new Timestamp(timestamp.getTime() + TimeUnit.HOURS.toMillis(1));
    Snooze snooze =
        Snooze.builder()
            .namespaceId(namespace1.getId())
            .definition(definition)
            .startTime(timestamp)
            .endTime(plus1Hr)
            .build();
    snooze.setCreatedBy(regularMember);
    snooze.setCreatedTime(timestamp);
    snooze.setUpdatedBy(regularMember);
    snooze.setUpdatedTime(timestamp);

    long id = dbUtil.create(snooze);
    snooze.setId(id);

    assumeTrue(dbUtil.getSnooze(id).isPresent());

    String url = baseUrl + "/api/v1/snooze/" + id;
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(url);
    assertEquals(200, response.getStatusCode());
    SnoozeView actual = response.as(SnoozeView.class);

    assertSnoozeEquals(snooze, actual, namespace1);
  }

  @Test
  void getByInvalidId() {
    long invalidId = -1;
    assumeFalse(dbUtil.getSnooze(invalidId).isPresent());

    String url = baseUrl + "/api/v1/snooze/" + invalidId;
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(url);
    assertEquals(404, response.getStatusCode());
    assertEquals("Snooze not found with id: " + invalidId, response.getBody().asString());
  }

  @Test
  void deleteSnooze() throws IOException {
    Map<String, Object> definition =
        new HashMap() {
          {
            put(ALERTIDS, Arrays.asList(1, 2));
          }
        };
    Timestamp plus1Hr = new Timestamp(timestamp.getTime() + TimeUnit.HOURS.toMillis(1));
    Snooze snooze =
        Snooze.builder()
            .namespaceId(namespace1.getId())
            .definition(definition)
            .startTime(timestamp)
            .endTime(plus1Hr)
            .build();
    snooze.setCreatedBy(regularMember);
    snooze.setCreatedTime(timestamp);
    snooze.setUpdatedBy(regularMember);
    snooze.setUpdatedTime(timestamp);

    long id = dbUtil.create(snooze);
    snooze.setId(id);

    assumeTrue(dbUtil.getSnooze(id).isPresent());

    String url = baseUrl + "/api/v1/snooze/delete";
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(Arrays.asList(id))
            .put(url);
    assertEquals(200, response.getStatusCode());

    assumeFalse(dbUtil.getSnooze(id).isPresent());
  }

  @Test
  void deleteByInvalidId() {
    long invalidId = -31;
    assumeFalse(dbUtil.getSnooze(invalidId).isPresent());

    String url = baseUrl + "/api/v1/snooze/delete";
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(Arrays.asList(invalidId))
            .put(url);
    assertEquals(404, response.getStatusCode());
    assertEquals("Snooze not found with id: " + invalidId, response.getBody().asString());
  }

  private Stream<Arguments> buildSnooze() {
    Timestamp plus1Hr = new Timestamp(timestamp.getTime() + TimeUnit.HOURS.toMillis(1));

    SnoozeView snooze1 = new SnoozeView();
    snooze1.setAlertIds(Arrays.asList(1, 2));
    snooze1.setEndTime(plus1Hr);

    SnoozeView snooze2 = new SnoozeView();
    snooze2.setAlertIds(Arrays.asList(3, 4));
    snooze2.setEndTime(plus1Hr);
    
    Map<String, Object> filter = new HashMap(){{put("key", "value");}};
    List<String> labels = Arrays.asList("test", "regression test");
    String reason = "regression testing";

    SnoozeView snooze1WithFilter = new SnoozeView();
    snooze1WithFilter.setAlertIds(Arrays.asList(1, 2));
    snooze1WithFilter.setEndTime(plus1Hr);
    snooze1WithFilter.setFilter(filter);
    snooze1WithFilter.setLabels(labels);
    snooze1WithFilter.setReason(reason);

    SnoozeView snooze2WithFilter = new SnoozeView();
    snooze2WithFilter.setAlertIds(Arrays.asList(3, 4));
    snooze2WithFilter.setEndTime(plus1Hr);
    snooze2WithFilter.setFilter(filter);
    snooze2WithFilter.setLabels(labels);
    snooze2WithFilter.setReason(reason);

    return Stream.of(
        arguments(Arrays.asList(snooze2), "one snooze"),
        arguments(Arrays.asList(snooze1, snooze2), "multiple snoozes"),
        arguments(Arrays.asList(snooze1WithFilter), "one snooze with body"),
        arguments(Arrays.asList(snooze1WithFilter, snooze2WithFilter), "multiple snoozes with body"));
  }

  private void assertSnoozeEquals(Snooze expected, SnoozeView actual, Namespace namespace) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(namespace.getName(), actual.getNamespace());
    Map<String, Object> definition = expected.getDefinition();
    assertEquals(definition.get(ALERTIDS), actual.getAlertIds());
    assertEquals(definition.get(REASON), actual.getReason());

    assertEquals(definition.get(NOTIFICATION), actual.getNotification());

    if (definition.get(LABELS) == null) {
      assertEquals(EMPTY_LIST, actual.getLabels());
    } else {
      assertEquals(definition.get(LABELS), actual.getLabels());
    }

    if (definition.get(FILTER) == null) {
      assertEquals(EMPTY_MAP, actual.getFilter());
    } else {
      assertEquals(definition.get(FILTER), actual.getFilter());
    }

    assertEquals(expected.getCreatedBy(), actual.getCreatedBy());
    assertEquals(expected.getCreatedTime(), actual.getCreatedTime());
    assertEquals(expected.getUpdatedBy(), actual.getUpdatedBy());
    assertEquals(expected.getUpdatedTime(), actual.getUpdatedTime());
  }

  private void assertSnoozeEquals(SnoozeView expected, Snooze actual, Namespace namespace) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(namespace.getId(), actual.getNamespaceId());
    Map<String, Object> definition = actual.getDefinition();
    assertEquals(expected.getNotification(), definition.get(NOTIFICATION));
    if (expected.getLabels().equals(EMPTY_LIST)) {
      assertNull(definition.get(LABELS));
    } else {
      assertEquals(expected.getLabels(), definition.get(LABELS));
    }
    if (expected.getFilter().equals(EMPTY_MAP)) {
      assertNull(definition.get(FILTER));
    } else {
      assertEquals(expected.getFilter(), definition.get(FILTER));
    }
    assertEquals(expected.getAlertIds(), definition.get(ALERTIDS));
    assertEquals(expected.getReason(), definition.get(REASON));

    assertEquals(expected.getCreatedBy(), actual.getCreatedBy());
    assertEquals(expected.getCreatedTime(), actual.getCreatedTime());
    assertEquals(expected.getUpdatedBy(), actual.getUpdatedBy());
    assertEquals(expected.getUpdatedTime(), actual.getUpdatedTime());
  }

  private void assertSnoozeEquals(
      SnoozeView expected,
      SnoozeView actual,
      Namespace namespace,
      String userId,
      Timestamp timestamp) {
    assertEquals(namespace.getName(), actual.getNamespace());

    assertEquals(expected.getAlertIds(), actual.getAlertIds());
    assertEquals(expected.getEndTime(), actual.getEndTime());
    if (expected.getStartTime() == null) {
      assertTrue(actual.getStartTime().getTime() >= timestamp.getTime());
    } else {
      assertEquals(expected.getStartTime(), actual.getEndTime());
    }
    assertTrue(actual.getStartTime().getTime() < actual.getEndTime().getTime());

    if (expected.getFilter() == null) {
      assertEquals(EMPTY_MAP, actual.getFilter());
    } else {
      assertEquals(expected.getFilter(), actual.getFilter());
    }

    if (expected.getLabels() == null) {
      assertEquals(EMPTY_LIST, actual.getLabels());
    } else {
      assertEquals(expected.getLabels(), actual.getLabels());
    }

    assertNull(actual.getNotification());

    assertEquals(expected.getReason(), actual.getReason());

    assertTrue(actual.getId() > 0);
    assertEquals(userId, actual.getCreatedBy());
    assertTrue(timestamp.getTime() <= actual.getCreatedTime().getTime());
    assertEquals(userId, actual.getUpdatedBy());
    assertEquals(actual.getCreatedTime(), actual.getUpdatedTime());
  }
}
