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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Response;
import net.opentsdb.horizon.model.Alert;
import net.opentsdb.horizon.model.AlertType;
import net.opentsdb.horizon.model.Contact;
import net.opentsdb.horizon.model.ContactType;
import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.service.AlertService;
import net.opentsdb.horizon.view.AlertView;
import net.opentsdb.horizon.view.BaseContact;
import net.opentsdb.horizon.view.BatchContact;
import net.opentsdb.horizon.view.EmailContact;
import net.opentsdb.horizon.view.HttpContact;
import net.opentsdb.horizon.view.OCContact;
import net.opentsdb.horizon.view.OpsGenieContact;
import net.opentsdb.horizon.view.SlackContact;
import net.opentsdb.horizon.util.Utils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
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
import static net.opentsdb.horizon.converter.AlertConverter.CREATED_FROM;
import static net.opentsdb.horizon.converter.AlertConverter.GROUPING_RULES;
import static net.opentsdb.horizon.converter.AlertConverter.NOTIFICATION;
import static net.opentsdb.horizon.converter.AlertConverter.QUERIES;
import static net.opentsdb.horizon.converter.AlertConverter.RECIPIENTS;
import static net.opentsdb.horizon.converter.AlertConverter.THRESHOLD;
import static net.opentsdb.horizon.converter.AlertConverter.VERSION;
import static net.opentsdb.horizon.integration.ContactIT.assertContactEquals;
import static net.opentsdb.horizon.integration.ContactIT.buildEmailContact;
import static net.opentsdb.horizon.integration.ContactIT.buildHttpContact;
import static net.opentsdb.horizon.integration.ContactIT.buildOCContact;
import static net.opentsdb.horizon.integration.ContactIT.buildOpsGenieContact;
import static net.opentsdb.horizon.integration.ContactIT.buildSlackContact;
import static net.opentsdb.horizon.integration.ContactIT.toModel;
import static net.opentsdb.horizon.converter.BaseConverter.NOT_PASSED;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.CREATED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class AlertIT extends BaseIT {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private Timestamp timestamp;
  private Namespace namespace1;
  private Namespace namespace2;

  @Override
  protected String getUri() {
    return "namespace";
  }

  @BeforeAll
  public void before() throws IOException {
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
  public void beforeMethod() {
    cascadeDeleteAlert();
  }

  @ParameterizedTest(name = "[{index}] create alert {1}")
  @MethodSource("buildAlerts")
  void createSimpleAlert(AlertView alertView, String displayName) throws IOException {

    BatchContact bc = (BatchContact) alertView.getNotification().get(RECIPIENTS);
    if (bc != null) {
      insertContact(bc, alertView.getNamespaceId(), regularMember, timestamp);
    }

    List<AlertView> expectedList = Arrays.asList(alertView);

    String url = endPoint + "/" + namespace1.getName() + "/alert";
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(expectedList)
            .post(url);
    assertEquals(201, response.getStatusCode());

    List<AlertView> actualList = Arrays.asList(response.as(AlertView[].class));

    assertAlertEquals(expectedList, actualList, regularMember, timestamp);

    AlertView actual = actualList.get(0);
    Alert actualFromDB = dbUtil.getAlert(actual.getId());
    assertAlertEquals(actual, actualFromDB);

    List<Contact> contacts = dbUtil.getContactForNamespace(namespace1.getId());
    BatchContact batchContact =
        OBJECT_MAPPER.convertValue(actual.getNotification().get(RECIPIENTS), BatchContact.class);

    if (batchContact == null) {
      assertEquals(0, contacts.size()); // empty recipient
    } else {
      ContactIT.assertContactEquals(batchContact, contacts, regularMember, timestamp);
    }
  }

  @Test
  void alertNameIsUniqueForANamespace() throws IOException {
    Map<String, Object> definition = buildDefinition();
    definition.put(VERSION, 3);
    String name = "test alert";
    Alert alert =
        buildAlert(
            name,
            AlertType.simple,
            true,
            false,
            namespace1.getId(),
            definition,
            null,
            "user.u1",
            timestamp);
    long id = insertAlert(alert);
    alert.setId(id);

    AlertView alertView =
        buildAlertView(
            name,
            AlertType.simple,
            true,
            namespace1.getId(),
            (Map<String, Object>) definition.get(QUERIES),
            (Map<String, Object>) definition.get(THRESHOLD),
            (Map<String, Object>) definition.get(NOTIFICATION),
            (List<String>) definition.get(GROUPING_RULES),
            (Map<String, Object>) definition.get(CREATED_FROM),
            new ArrayList<>(),
            3);

    String url = endPoint + "/" + namespace1.getName() + "/alert";
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(Arrays.asList(alertView))
            .post(url);
    assertEquals(CONFLICT.getStatusCode(), response.getStatusCode());
    assertEquals("Duplicate alert name: " + name, response.getBody().asString());

    alertView.setName(name + " 2");
    alertView.setNamespaceId(namespace2.getId());
    url = endPoint + "/" + namespace2.getName() + "/alert";
    response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(Arrays.asList(alertView))
            .post(url);
    assertEquals(CREATED.getStatusCode(), response.getStatusCode());
  }

  @Test
  void updateAlert() throws IOException {
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    Map<String, Object> definition = buildDefinition();
    definition.put(VERSION, 3);
    Alert alert =
        buildAlert(
            "test alert",
            AlertType.simple,
            true,
            false,
            namespace1.getId(),
            definition,
            null,
            "user.u1",
            timestamp);
    long id = insertAlert(alert);
    alert.setId(id);

    EmailContact emailContact = buildEmailContact("test@test.com", false);
    BatchContact batchContact = new BatchContact();
    batchContact.setEmail(Arrays.asList(emailContact));
    insertContact(batchContact.getEmail(), namespace1.getId(), regularMember, timestamp);

    AlertView view = toView(alert);
    view.setName("new" + view.getName());
    view.setLabels(Arrays.asList("integration test", "test"));
    view.getQueries().put("k1", "v1");
    view.getThreshold().put("k1", "v1");
    view.getNotification().put("k1", "v1");
    view.getAlertGroupingRules().add("new rule");
    view.setVersion(4);
    view.getNotification().put(RECIPIENTS, batchContact);

    List<AlertView> expectedList = Arrays.asList(view);
    String url = endPoint + "/" + namespace1.getName() + "/alert";
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(expectedList)
            .put(url);
    assertEquals(200, response.getStatusCode());
    List<AlertView> actualList = Arrays.asList(response.as(AlertView[].class));
    assertAlertEquals(expectedList, actualList, regularMember, timestamp);

    AlertView actual = actualList.get(0);
    Alert actualFromDB = dbUtil.getAlert(actual.getId());
    assertAlertEquals(actual, actualFromDB);

    List<Contact> contacts = dbUtil.getAlertContacts(actual.getId());
    BatchContact expectedContact =
        OBJECT_MAPPER.convertValue(actual.getNotification().get(RECIPIENTS), BatchContact.class);
    assertContactEquals(expectedContact, contacts, regularMember, timestamp);
  }

  @Test
  void cannotUpdateAlertWithDuplicateName() throws IOException {
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    Map<String, Object> definition = buildDefinition();
    definition.put(VERSION, 3);
    String name1 = "test alert1";
    String name2 = "test alert2";
    Alert alert1 =
        buildAlert(
            name1,
            AlertType.simple,
            true,
            false,
            namespace1.getId(),
            definition,
            null,
            "user.u1",
            timestamp);
    Alert alert2 =
        buildAlert(
            name2,
            AlertType.simple,
            true,
            false,
            namespace1.getId(),
            definition,
            null,
            "user.u1",
            timestamp);
    long id1 = insertAlert(alert1);
    long id2 = insertAlert(alert2);
    alert1.setId(id1);
    alert2.setId(id2);

    EmailContact emailContact = buildEmailContact("test@test.com", false);
    BatchContact batchContact = new BatchContact();
    batchContact.setEmail(Arrays.asList(emailContact));
    insertContact(batchContact.getEmail(), namespace1.getId(), regularMember, timestamp);

    AlertView view = toView(alert1);
    view.setName(alert2.getName());
    view.setLabels(Arrays.asList("integration test", "test"));
    view.getQueries().put("k1", "v1");
    view.getThreshold().put("k1", "v1");
    view.getNotification().put("k1", "v1");
    view.getAlertGroupingRules().add("new rule");
    view.setVersion(4);
    view.getNotification().put(RECIPIENTS, batchContact);

    List<AlertView> expectedList = Arrays.asList(view);
    String url = endPoint + "/" + namespace1.getName() + "/alert";
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(expectedList)
            .put(url);
    assertEquals(CONFLICT.getStatusCode(), response.getStatusCode());
    assertEquals("Duplicate alert name: " + view.getName(), response.getBody().asString());
  }

  @ParameterizedTest(name = "[{index}] {4}")
  @MethodSource("alertRecipientSource")
  void updateAlertRecipientList(
      final Alert alert,
      BatchContact contactList,
      BatchContact originalRecipients,
      BatchContact newRecipients,
      String display)
      throws IOException {

    long id = insertAlert(alert);
    alert.setId(id);

    insertContact(
        contactList, alert.getNamespaceId(), alert.getCreatedBy(), alert.getCreatedTime());
    insertAlertContact(id, originalRecipients);

    AlertView view = toView(alert);
    view.getNotification().put(RECIPIENTS, newRecipients);
    List<AlertView> expectedList = Arrays.asList(view);
    String url = endPoint + "/" + namespace1.getName() + "/alert";
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(expectedList)
            .put(url);
    assertEquals(200, response.getStatusCode());
    List<AlertView> actualList = Arrays.asList(response.as(AlertView[].class));
    assertAlertEquals(expectedList, actualList, regularMember, timestamp);

    AlertView actual = actualList.get(0);
    List<Contact> contacts = dbUtil.getAlertContacts(actual.getId());
    BatchContact expectedContact =
        OBJECT_MAPPER.convertValue(actual.getNotification().get(RECIPIENTS), BatchContact.class);
    assertContactEquals(expectedContact, contacts, alert.getCreatedBy(), timestamp);
  }

  @Test
  void disableAlert() throws IOException {

    Map<String, Object> definition = buildDefinition();
    definition.put(VERSION, 5);

    Alert alert =
        buildAlert(
            "test alert",
            AlertType.simple,
            true,
            false,
            namespace1.getId(),
            definition,
            null,
            regularMember,
            timestamp);
    long id = insertAlert(alert);

    AlertView alertView = new AlertView();
    alertView.setId(id);
    alertView.setEnabled(false);

    String url = endPoint + "/" + namespace1.getName() + "/alert";
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(new AlertView[] {alertView})
            .put(url);
    assertEquals(200, response.getStatusCode());

    alert.setId(id);
    alert.setEnabled(false);
    Alert actualFromDB = dbUtil.getAlert(id);
    assertAlert(alert, actualFromDB);
  }

  @Test
  void enableAlert() throws IOException {

    Map<String, Object> definition = buildDefinition();
    definition.put(VERSION, 1);

    Alert alert =
        buildAlert(
            "test alert",
            AlertType.simple,
            false,
            false,
            namespace1.getId(),
            definition,
            new ArrayList<>(),
            regularMember,
            timestamp);
    long id = insertAlert(alert);

    AlertView alertView = new AlertView();
    alertView.setId(id);
    alertView.setEnabled(true);

    String url = endPoint + "/" + namespace1.getName() + "/alert";
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(new AlertView[] {alertView})
            .put(url);
    assertEquals(200, response.getStatusCode());

    alert.setId(id);
    alert.setEnabled(true);
    Alert actualFromDB = dbUtil.getAlert(id);
    assertAlert(alert, actualFromDB);
  }

  @Test
  void enableAnOldAlertWithRecipientInBody() throws IOException {

    EmailContact email = buildEmailContact("u1@test.com", false);
    BatchContact batchContact = new BatchContact();
    batchContact.setEmail(Arrays.asList(email));

    Map<String, Object> definition = buildDefinition();
    ((Map<String, Object>) definition.get(NOTIFICATION)).put(RECIPIENTS, batchContact);
    definition.put(VERSION, 1);

    Alert alert =
        buildAlert(
            "test alert",
            AlertType.simple,
            false,
            false,
            namespace1.getId(),
            definition,
            new ArrayList<>(),
            regularMember,
            timestamp);
    long id = insertAlert(alert);
    int contactId = dbUtil.insert(toModel(email, namespace1.getId(), regularMember, timestamp));
    email.setId(contactId);
    dbUtil.insertAlertContact(id, Arrays.asList(contactId));

    AlertView alertView = new AlertView();
    alertView.setId(id);
    alertView.setEnabled(true);

    String url = endPoint + "/" + namespace1.getName() + "/alert";
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(new AlertView[] {alertView})
            .put(url);
    assertEquals(200, response.getStatusCode());

    alert.setId(id);
    alert.setEnabled(true);
    Alert actualFromDB = dbUtil.getAlert(id);

    ((Map<String, Object>) alert.getDefinition().get(NOTIFICATION)).remove(RECIPIENTS);
    ((Map<String, Object>) actualFromDB.getDefinition().get(NOTIFICATION)).remove(RECIPIENTS);

    assertAlert(alert, actualFromDB);
  }

  @ParameterizedTest(name = "[{index}] get alert with {1}")
  @MethodSource("net.opentsdb.horizon.integration.ContactIT#buildContacts")
  public void getById(BatchContact batchContact, String display) throws IOException {

    int namespaceId = namespace1.getId();
    Map<String, Object> definition = buildDefinition();
    definition.put(VERSION, 5);
    List<String> labels = Arrays.asList("integration test", "test");

    Alert alert =
        buildAlert(
            "test alert",
            AlertType.simple,
            true,
            false,
            namespaceId,
            definition,
            labels,
            regularMember,
            timestamp);
    long id = insertAlert(alert);
    alert.setId(id);

    if (batchContact != null) {
      insertContact(batchContact, namespaceId, regularMember, timestamp);
      insertAlertContact(id, batchContact);
    }

    String url = baseUrl + "/api/v1/alert/" + id;

    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(url)
            .andReturn();
    assertEquals(200, response.getStatusCode());
    AlertView actual = response.as(AlertView.class);

    assertAlertWithDefinition(definition, alert, batchContact, actual);
  }

  @ParameterizedTest(name = "[{index}] get definition: {0}, deleted: {0}")
  @CsvSource({"true, true", "false, true", "true, false", "false, false"})
  void getDeletedById(final boolean fetchDefinition, final boolean fetchDeleted)
      throws IOException {

    int namespaceId = namespace1.getId();
    Map<String, Object> definition = buildDefinition();
    definition.put(VERSION, 5);
    List<String> labels = Arrays.asList("integration test", "test");

    Alert alert =
        buildAlert(
            "test alert",
            AlertType.simple,
            true,
            true,
            namespaceId,
            definition,
            labels,
            regularMember,
            timestamp);
    long id = insertAlert(alert);
    alert.setId(id);

    EmailContact emailContact = buildEmailContact("test@test.com", false);
    BatchContact batchContact = new BatchContact();
    batchContact.setEmail(Arrays.asList(emailContact));
    insertContact(batchContact, namespaceId, regularMember, timestamp);
    insertAlertContact(id, batchContact);

    String url =
        baseUrl
            + "/api/v1/alert/"
            + id
            + "?definition="
            + fetchDefinition
            + "&deleted="
            + fetchDeleted;
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(url)
            .andReturn();

    if (fetchDeleted) {
      assertEquals(200, response.getStatusCode());
      AlertView actual = response.as(AlertView.class);
      if (fetchDefinition) {
        assertAlertWithDefinition(definition, alert, batchContact, actual);
      } else {
        assertAlertWithoutDefinition(batchContact, alert, actual);
      }
    } else {
      assertEquals(404, response.getStatusCode());
    }
  }

  @ParameterizedTest(name = "[{index}] get alert with {1}")
  @MethodSource("net.opentsdb.horizon.integration.ContactIT#buildContacts")
  public void getByIdWithOutDefinition(BatchContact batchContact, String display)
      throws IOException {
    int namespaceId = namespace1.getId();
    Map<String, Object> definition = buildDefinition();
    definition.put(VERSION, 5);
    List<String> labels = Arrays.asList("integration test", "test");

    Alert alert =
        buildAlert(
            "test alert",
            AlertType.simple,
            true,
            false,
            namespaceId,
            definition,
            labels,
            regularMember,
            timestamp);
    long id = insertAlert(alert);
    alert.setId(id);

    if (batchContact != null) {
      insertContact(batchContact, namespaceId, regularMember, timestamp);
      insertAlertContact(id, batchContact);
    }

    String url = baseUrl + "/api/v1/alert/" + id + "?definition=false";
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(url)
            .andReturn();
    assertEquals(200, response.getStatusCode());
    AlertView actual = response.as(AlertView.class);

    assertAlertWithoutDefinition(batchContact, alert, actual);
  }

  @ParameterizedTest(name = "[{index}] get alert with {1}")
  @MethodSource("net.opentsdb.horizon.integration.ContactIT#buildContacts")
  public void getByNamespaceWithOutDefinition(BatchContact batchContact, String display)
      throws IOException {
    int namespaceId = namespace1.getId();
    Map<String, Object> definition = buildDefinition();
    definition.put(VERSION, 5);
    List<String> labels = Arrays.asList("integration test", "test");

    Alert alert =
        buildAlert(
            "test alert",
            AlertType.simple,
            true,
            false,
            namespaceId,
            definition,
            labels,
            regularMember,
            timestamp);
    long id = insertAlert(alert);
    alert.setId(id);

    if (batchContact != null) {
      insertContact(batchContact, namespaceId, regularMember, timestamp);
      insertAlertContact(id, batchContact);
    }

    String url = endPoint + "/" + namespace1 + "/alert" + "?definition=false";
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(url)
            .andReturn();
    assertEquals(200, response.getStatusCode());
    List<AlertView> actualList = Arrays.asList(response.as(AlertView[].class));
    assertEquals(1, actualList.size());
    AlertView actual = actualList.get(0);

    assertAlertWithoutDefinition(batchContact, alert, actual);
  }

  @ParameterizedTest(name = "[{index}] get alert with {1}")
  @MethodSource("net.opentsdb.horizon.integration.ContactIT#buildContacts")
  public void getByNamespaceWithDefinition(BatchContact batchContact, String display)
      throws IOException {
    int namespaceId = namespace1.getId();
    Map<String, Object> definition = buildDefinition();
    definition.put(VERSION, 5);
    List<String> labels = Arrays.asList("integration test", "test");

    Alert alert =
        buildAlert(
            "test alert",
            AlertType.simple,
            true,
            false,
            namespaceId,
            definition,
            labels,
            regularMember,
            timestamp);
    long id = insertAlert(alert);
    alert.setId(id);

    if (batchContact != null) {
      insertContact(batchContact, namespaceId, regularMember, timestamp);
      insertAlertContact(id, batchContact);
    }

    String url = endPoint + "/" + namespace1 + "/alert" + "?definition=true";

    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(url)
            .andReturn();
    assertEquals(200, response.getStatusCode());
    List<AlertView> actualList = Arrays.asList(response.as(AlertView[].class));
    assertEquals(1, actualList.size());
    AlertView actual = actualList.get(0);

    assertAlertWithDefinition(definition, alert, batchContact, actual);
  }

  @ParameterizedTest(name = "[{index}] get definition: {0}, deleted: {0}")
  @CsvSource({"true, true", "false, true", "true, false", "false, false"})
  void getDeletedByNamespace(final boolean fetchDefinition, final boolean fetchDeleted)
      throws IOException {

    int namespaceId = namespace1.getId();
    Map<String, Object> definition = buildDefinition();
    definition.put(VERSION, 5);
    List<String> labels = Arrays.asList("integration test", "test");

    Alert alert =
        buildAlert(
            "test alert",
            AlertType.simple,
            true,
            true,
            namespaceId,
            definition,
            labels,
            regularMember,
            timestamp);
    long id = insertAlert(alert);
    alert.setId(id);

    EmailContact emailContact = buildEmailContact("test@test.com", false);
    BatchContact batchContact = new BatchContact();
    batchContact.setEmail(Arrays.asList(emailContact));

    insertContact(batchContact, namespaceId, regularMember, timestamp);
    insertAlertContact(id, batchContact);

    String url =
        endPoint
            + "/"
            + namespace1
            + "/alert"
            + "?definition="
            + fetchDefinition
            + "&deleted="
            + fetchDeleted;
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(url)
            .andReturn();

    if (fetchDeleted) {
      assertEquals(200, response.getStatusCode());
      AlertView[] actualList = response.as(AlertView[].class);
      assertEquals(1, actualList.length);
      AlertView actual = actualList[0];
      if (fetchDefinition) {
        assertAlertWithDefinition(definition, alert, batchContact, actual);
      } else {
        assertAlertWithoutDefinition(batchContact, alert, actual);
      }
    } else {
      assertEquals(200, response.getStatusCode());
      AlertView[] actualList = response.as(AlertView[].class);
      assertEquals(0, actualList.length);
    }
  }

  @ParameterizedTest(name = "[{index}] get definition: {0}, deleted: {0}")
  @CsvSource({"true, true", "false, true", "true, false", "false, false"})
  void getDeletedByName(final boolean fetchDefinition, final boolean fetchDeleted)
      throws IOException {
    int namespaceId = namespace1.getId();
    Map<String, Object> definition = buildDefinition();
    definition.put(VERSION, 5);
    List<String> labels = Arrays.asList("integration test", "test");

    String name = "test alert";
    Alert alert =
        buildAlert(
            name,
            AlertType.simple,
            true,
            true,
            namespaceId,
            definition,
            labels,
            regularMember,
            timestamp);
    long id = insertAlert(alert);
    alert.setId(id);

    EmailContact emailContact = buildEmailContact("test@test.com", false);
    BatchContact batchContact = new BatchContact();
    batchContact.setEmail(Arrays.asList(emailContact));

    insertContact(batchContact, namespaceId, regularMember, timestamp);
    insertAlertContact(id, batchContact);

    String url =
        endPoint
            + "/"
            + namespace1
            + "/alert/"
            + name
            + "?definition="
            + fetchDefinition
            + "&deleted="
            + fetchDeleted;
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(url)
            .andReturn();

    if (fetchDeleted) {
      assertEquals(200, response.getStatusCode());
      AlertView actual = response.as(AlertView.class);
      if (fetchDefinition) {
        assertAlertWithDefinition(definition, alert, batchContact, actual);
      } else {
        assertAlertWithoutDefinition(batchContact, alert, actual);
      }
    } else {
      assertEquals(404, response.getStatusCode());
    }
  }

  @Test
  void softDeleteAlert() throws IOException {

    Map<String, Object> definition = buildDefinition();
    definition.put(VERSION, 3);
    Alert alert1 =
        buildAlert(
            "test alert1",
            AlertType.simple,
            true,
            false,
            namespace1.getId(),
            definition,
            Arrays.asList("test"),
            "user.u1",
            timestamp);

    Alert alert2 =
        buildAlert(
            "test alert2",
            AlertType.simple,
            true,
            false,
            namespace1.getId(),
            definition,
            Arrays.asList("test"),
            "user.u1",
            timestamp);

    long id1 = dbUtil.createAlert(alert1);
    long id2 = dbUtil.createAlert(alert2);
    alert1.setId(id1);
    alert2.setId(id2);

    EmailContact email = buildEmailContact("e1@test.com", false);
    SlackContact slack = buildSlackContact("slack", "webhook");
    BatchContact batchContact = new BatchContact();
    batchContact.setEmail(Arrays.asList(email));
    batchContact.setSlack(Arrays.asList(slack));

    insertContact(batchContact, namespace1.getId(), alert1.getCreatedBy(), alert1.getCreatedTime());
    insertAlertContact(id1, batchContact);
    insertAlertContact(id2, batchContact);

    assumeFalse(dbUtil.getAlert(id1).isDeleted());
    assumeFalse(dbUtil.getAlert(id2).isDeleted());

    String url = endPoint + "/" + namespace1.getName() + "/alert/delete";
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(new long[] {id1, id2})
            .put(url);
    assertEquals(204, response.getStatusCode());

    Alert actual1 = dbUtil.getAlert(id1);
    Alert actual2 = dbUtil.getAlert(id2);
    assertTrue(actual1.isDeleted());
    String newName = actual1.getName();
    long timeStamp = Long.parseLong(newName.substring(newName.lastIndexOf("-") + 1));
    assertEquals(timeStamp, actual1.getUpdatedTime().getTime());
    assertEquals(regularMember, actual1.getUpdatedBy());

    assertTrue(actual2.isDeleted());
    newName = actual1.getName();
    timeStamp = Long.parseLong(newName.substring(newName.lastIndexOf("-") + 1));
    assertEquals(timeStamp, actual2.getUpdatedTime().getTime());
    assertEquals(regularMember, actual2.getUpdatedBy());

    assertAlertEquals(alert1, actual1);
    assertAlertEquals(alert2, actual2);

    // verify the contact association is removed
    assertEquals(0, dbUtil.getContactIdsForAlert(id1).size());
    assertEquals(0, dbUtil.getContactIdsForAlert(id2).size());

    // verify the contact itself is not removed
    assertTrue(dbUtil.getContactById(email.getId()).isPresent());
    assertTrue(dbUtil.getContactById(slack.getId()).isPresent());
  }

  @Test
  void restoreAlert() throws IOException {
    Map<String, Object> definition = buildDefinition();
    definition.put(VERSION, 3);
    Alert alert1 =
        buildAlert(
            "test alert1",
            AlertType.simple,
            true,
            true,
            namespace1.getId(),
            definition,
            Arrays.asList("test"),
            "user.u1",
            timestamp);

    Alert alert2 =
        buildAlert(
            "test alert2",
            AlertType.simple,
            true,
            true,
            namespace1.getId(),
            definition,
            Arrays.asList("test"),
            "user.u1",
            timestamp);

    long id1 = dbUtil.createAlert(alert1);
    long id2 = dbUtil.createAlert(alert2);
    alert1.setId(id1);
    alert2.setId(id2);

    EmailContact email = buildEmailContact("e1@test.com", false);
    SlackContact slack = buildSlackContact("slack", "webhook");
    BatchContact batchContact = new BatchContact();
    batchContact.setEmail(Arrays.asList(email));
    batchContact.setSlack(Arrays.asList(slack));

    insertContact(batchContact, namespace1.getId(), alert1.getCreatedBy(), alert1.getCreatedTime());
    insertAlertContact(id1, batchContact);
    insertAlertContact(id2, batchContact);

    assumeTrue(dbUtil.getAlert(id1).isDeleted());
    assumeTrue(dbUtil.getAlert(id2).isDeleted());

    String url = endPoint + "/" + namespace1.getName() + "/alert/restore";
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(new long[] {id1, id2})
            .put(url);
    assertEquals(204, response.getStatusCode());

    Alert actual1 = dbUtil.getAlert(id1);
    Alert actual2 = dbUtil.getAlert(id2);
    assertFalse(actual1.isDeleted());
    assertEquals(regularMember, actual1.getUpdatedBy());
    assertFalse(actual2.isDeleted());
    assertEquals(regularMember, actual2.getUpdatedBy());
    assertAlertEquals(alert1, actual1);
    assertAlertEquals(alert2, actual2);

    List<Integer> expected = Arrays.asList(email.getId(), slack.getId());
    assertEquals(expected, dbUtil.getContactIdsForAlert(id1));
    assertEquals(expected, dbUtil.getContactIdsForAlert(id2));
  }

  @Test
  void updateAlertAfterRecipientNameChange() throws IOException {

    EmailContact email = buildEmailContact("e1@test.com", false);
    String slackName = "slack";
    SlackContact slack = buildSlackContact(slackName, "webhook");
    BatchContact batchContact = new BatchContact();
    batchContact.setEmail(Arrays.asList(email));
    batchContact.setSlack(Arrays.asList(slack));

    Map<String, Object> definition = buildDefinition();
    definition.put(VERSION, 3);
    ((Map) definition.get(NOTIFICATION)).put(RECIPIENTS, batchContact);
    Alert alert =
        buildAlert(
            "test alert1",
            AlertType.simple,
            true,
            false,
            namespace1.getId(),
            definition,
            Arrays.asList("test"),
            "user.u1",
            timestamp);

    long id = dbUtil.createAlert(alert);
    alert.setId(id);

    String newSlackName = "new " + slackName;
    slack.setName(newSlackName);
    insertContact(batchContact, namespace1.getId(), alert.getCreatedBy(), alert.getCreatedTime());
    insertAlertContact(id, batchContact);

    assumeTrue(newSlackName.equals(dbUtil.getContactById(slack.getId()).get().getName()));

    AlertView view = toView(alert);
    view.setName("new" + view.getName());

    List<AlertView> expectedList = Arrays.asList(view);
    String url = endPoint + "/" + namespace1.getName() + "/alert";
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(expectedList)
            .put(url);
    assertEquals(200, response.getStatusCode());
    List<AlertView> actualList = Arrays.asList(response.as(AlertView[].class));
    assertAlertEquals(expectedList, actualList, regularMember, timestamp);

    AlertView actual = actualList.get(0);
    Alert actualFromDB = dbUtil.getAlert(actual.getId());
    assertAlertEquals(actual, actualFromDB);
  }

  @Test
  void deleteAndCreateAlertWithSameName() throws IOException {
    Map<String, Object> definition = buildDefinition();
    definition.put(VERSION, 3);
    Alert alert =
        buildAlert(
            "test alert1",
            AlertType.simple,
            true,
            false,
            namespace1.getId(),
            definition,
            Arrays.asList("test"),
            "user.u1",
            timestamp);

    long id = dbUtil.createAlert(alert);
    alert.setId(id);

    String url = endPoint + "/" + namespace1.getName() + "/alert/delete";
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(new long[] {id})
            .put(url);
    assumeTrue(204 == response.getStatusCode());

    AlertView view = toView(alert);
    view.setId(NOT_PASSED);

    List<AlertView> expectedList = Arrays.asList(view);
    url = endPoint + "/" + namespace1.getName() + "/alert";
    response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(expectedList)
            .post(url);
    assertEquals(201, response.getStatusCode());
    List<AlertView> actualList = Arrays.asList(response.as(AlertView[].class));
    assertAlertEquals(expectedList, actualList, regularMember, timestamp);

    AlertView actual = actualList.get(0);
    Alert actualFromDB = dbUtil.getAlert(actual.getId());
    assertAlertEquals(actual, actualFromDB);
  }

  @Test
  void createAlertWithAdminEmailId() {

    Map<String, Object> definition = buildDefinition();

    AlertView alertView =
        buildAlertView(
            "test alert 1",
            AlertType.simple,
            true,
            namespace1.getId(),
            (Map<String, Object>) definition.get(QUERIES),
            (Map<String, Object>) definition.get(THRESHOLD),
            (Map<String, Object>) definition.get(NOTIFICATION),
            (List<String>) definition.get(GROUPING_RULES),
            (Map<String, Object>) definition.get(CREATED_FROM),
            new ArrayList<>(),
            3);

    String contactUrl = endPoint + "/" + namespace1.getName() + "/contact?type=" + ContactType.email;
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(contactUrl)
            .andReturn();
    assumeTrue(200 == response.getStatusCode());
    BatchContact recipients = response.getBody().as(BatchContact.class);
    List<EmailContact> emailContacts = recipients.getEmail();
    assumeFalse(emailContacts.isEmpty());
    emailContacts.stream()
        .forEach(
            emailContact -> {
              assumeTrue(emailContact.isAdmin());
              assumeTrue(emailContact.getId() == 0);
            });

    alertView.getNotification().put(RECIPIENTS, recipients);

    List<AlertView> expectedList = Arrays.asList(alertView);
    String url = endPoint + "/" + namespace1.getName() + "/alert";
    response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(expectedList)
            .post(url);
    assertEquals(201, response.getStatusCode());

    List<AlertView> actualList = Arrays.asList(response.as(AlertView[].class));

    assertAlertEquals(expectedList, actualList, regularMember, timestamp);

    AlertView actual = actualList.get(0);
    Alert actualFromDB = dbUtil.getAlert(actual.getId());
    assertAlertEquals(actual, actualFromDB);

    List<Contact> contacts = dbUtil.getContactForNamespace(namespace1.getId());
    BatchContact batchContact =
        OBJECT_MAPPER.convertValue(actual.getNotification().get(RECIPIENTS), BatchContact.class);
    ContactIT.assertContactEquals(batchContact, contacts, regularMember, timestamp);
  }

  @Test
  void updateAlertAndAddAdminEmailId() throws IOException {

    Map<String, Object> definition = buildDefinition();
    definition.put(VERSION, 3);
    Alert alert =
        buildAlert(
            "test alert",
            AlertType.simple,
            true,
            false,
            namespace1.getId(),
            definition,
            null,
            "user.u1",
            timestamp);
    long id = dbUtil.createAlert(alert);
    alert.setId(id);

    String contactUrl = endPoint + "/" + namespace1.getName() + "/contact?type=" + ContactType.email;
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(contactUrl)
            .andReturn();
    assumeTrue(200 == response.getStatusCode());
    BatchContact recipients = response.getBody().as(BatchContact.class);
    List<EmailContact> emailContacts = recipients.getEmail();
    assumeFalse(emailContacts.isEmpty());
    emailContacts.stream()
        .forEach(
            emailContact -> {
              assumeTrue(emailContact.isAdmin());
              assumeTrue(emailContact.getId() == 0);
            });

    AlertView alertView = toView(alert);
    alertView.getNotification().put(RECIPIENTS, recipients);

    List<AlertView> expectedList = Arrays.asList(alertView);
    String url = endPoint + "/" + namespace1.getName() + "/alert";
    response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(expectedList)
            .put(url);
    assertEquals(200, response.getStatusCode());

    List<AlertView> actualList = Arrays.asList(response.as(AlertView[].class));

    assertAlertEquals(expectedList, actualList, regularMember, timestamp);

    AlertView actual = actualList.get(0);
    Alert actualFromDB = dbUtil.getAlert(actual.getId());
    assertAlertEquals(actual, actualFromDB);

    List<Contact> contacts = dbUtil.getContactForNamespace(namespace1.getId());
    BatchContact batchContact =
        OBJECT_MAPPER.convertValue(actual.getNotification().get(RECIPIENTS), BatchContact.class);
    ContactIT.assertContactEquals(batchContact, contacts, regularMember, timestamp);
  }

  private void insertAlertContact(long alertId, BatchContact batchContact) {
    insertAlertContact(alertId, batchContact.getEmail());
    insertAlertContact(alertId, batchContact.getSlack());
    insertAlertContact(alertId, batchContact.getOpsgenie());
    insertAlertContact(alertId, batchContact.getHttp());
    insertAlertContact(alertId, batchContact.getOc());
  }

  private <T extends BaseContact> void insertAlertContact(long alertId, List<T> contacts) {
    if (!Utils.isNullOrEmpty(contacts)) {
      List<Integer> contactIds = contacts.stream().map(T::getId).collect(Collectors.toList());
      dbUtil.insertAlertContact(alertId, contactIds);
    }
  }

  private void insertContact(
      BatchContact batchContact, int namespaceId, String createBy, Timestamp createdTime)
      throws IOException {
    insertContact(batchContact.getEmail(), namespaceId, createBy, createdTime);
    insertContact(batchContact.getSlack(), namespaceId, createBy, createdTime);
    insertContact(batchContact.getOpsgenie(), namespaceId, createBy, createdTime);
    insertContact(batchContact.getHttp(), namespaceId, createBy, createdTime);
    insertContact(batchContact.getOc(), namespaceId, createBy, createdTime);
  }

  private <T extends BaseContact> List<Integer> insertContact(
      List<T> contacts, int namespaceId, String createdBy, Timestamp createdTime)
      throws IOException {
    List<Integer> ids = new ArrayList<>();
    if (!Utils.isNullOrEmpty(contacts)) {
      for (T t : contacts) {
        Contact contact = toModel(t, namespaceId, createdBy, createdTime);
        int id = dbUtil.insert(contact);
        t.setId(id);
        ids.add(id);
      }
    }
    return ids;
  }

  private AlertView toView(Alert model) {
    AlertView view = new AlertView();
    view.setId(model.getId());
    view.setName(model.getName());
    view.setType(model.getType());
    view.setEnabled(model.isEnabled());
    view.setDeleted(model.isDeleted());
    view.setNamespaceId(model.getNamespaceId());
    Map<String, Object> definition = model.getDefinition();
    if (!Utils.isNullOrEmpty(definition)) {
      view.setQueries((Map<String, Object>) definition.get(QUERIES));
      view.setThreshold((Map<String, Object>) definition.get(THRESHOLD));
      view.setNotification((Map<String, Object>) definition.get(NOTIFICATION));
      view.setAlertGroupingRules((List<String>) definition.get(GROUPING_RULES));
      view.setCreatedFrom((Map<String, Object>) definition.get(CREATED_FROM));
      view.setVersion((int) definition.get(VERSION));
    }
    view.setLabels(model.getLabels());
    view.setCreatedBy(model.getCreatedBy());
    view.setCreatedTime(model.getCreatedTime());
    view.setUpdatedBy(model.getUpdatedBy());
    view.setUpdatedTime(model.getUpdatedTime());
    return view;
  }

  private Stream<Arguments> alertRecipientSource() {

    Map<String, Object> definition = buildDefinition();
    definition.put(VERSION, 3);
    Alert alert =
        buildAlert(
            "test alert",
            AlertType.simple,
            true,
            false,
            namespace1.getId(),
            definition,
            null,
            "user.u1",
            timestamp);

    EmailContact email1 = buildEmailContact("e1@test.com", false);
    EmailContact email2 = buildEmailContact("e2@test.com", false);
    EmailContact email3 = buildEmailContact("e3@test.com", false);

    SlackContact slack1 = buildSlackContact("slack1", "webhook1");
    SlackContact slack2 = buildSlackContact("slack2", "webhook2");

    OpsGenieContact opsGenie1 = buildOpsGenieContact("opsgenie1", "apikey1");
    OpsGenieContact opsGenie2 = buildOpsGenieContact("opsgenie2", "apikey2");
    OpsGenieContact opsGenie3 = buildOpsGenieContact("opsgenie3", "apikey3");

    HttpContact http1 = buildHttpContact("http1", "endpoint1");
    HttpContact http2 = buildHttpContact("http2", "endpoint2");

    OCContact oc1 = buildOCContact("oc1", "context1", "customer1", "property1", "1");
    OCContact oc2 = buildOCContact("oc2", "context2", "customer2", "property2", "2");
    OCContact oc3 = buildOCContact("oc3", "context3", "customer3", "property3", "3");

    BatchContact allContacts = new BatchContact();
    allContacts.setEmail(Arrays.asList(email1, email2, email3));
    allContacts.setSlack(Arrays.asList(slack1, slack2));
    allContacts.setOpsgenie(Arrays.asList(opsGenie1, opsGenie2, opsGenie3));
    allContacts.setHttp(Arrays.asList(http1, http2));
    allContacts.setOc(Arrays.asList(oc1, oc2, oc3));

    // first batch
    BatchContact originalList1 = new BatchContact();
    originalList1.setEmail(Arrays.asList(email1, email2));
    originalList1.setSlack(Arrays.asList(slack1));

    BatchContact newList1 = new BatchContact();
    newList1.setEmail(new ArrayList<>(originalList1.getEmail()));
    newList1.setSlack(new ArrayList<>(originalList1.getSlack()));
    newList1.setOpsgenie(Arrays.asList(opsGenie1, opsGenie2));
    newList1.setHttp(Arrays.asList(http1));
    newList1.setOc(Arrays.asList(oc1, oc2, oc3));

    // second batch
    BatchContact originalList2 = newList1;

    BatchContact newList2 = new BatchContact();
    newList2.setEmail(Arrays.asList(email1));
    newList2.setSlack(null);
    newList2.setOpsgenie(Arrays.asList(opsGenie2));
    newList2.setHttp(null);
    newList2.setOc(Arrays.asList(oc2, oc3));

    // second batch
    BatchContact originalList3 = newList1;

    BatchContact newList3 = new BatchContact();
    newList3.setEmail(Arrays.asList(email2, email3));
    newList3.setSlack(Arrays.asList(slack2));
    newList3.setOpsgenie(Arrays.asList(opsGenie1, opsGenie3));
    newList3.setHttp(Arrays.asList(http2));
    newList3.setOc(null);

    return Stream.of(
        arguments(alert, allContacts, originalList1, newList1, "add recipients"),
        arguments(alert, allContacts, originalList2, newList2, "remove recipients"),
        arguments(alert, allContacts, originalList3, newList3, "add and remove recipients"));
  }

  private Stream<Arguments> buildAlerts() {
    String name = "test alert";
    Map<String, Object> definition = buildDefinition();
    Map<String, Object> queries = (Map<String, Object>) definition.get(QUERIES);
    Map<String, Object> threshold = (Map<String, Object>) definition.get(THRESHOLD);
    Map<String, Object> notification = (Map<String, Object>) definition.get(NOTIFICATION);
    List<String> groupingRules = (List<String>) definition.get(GROUPING_RULES);
    Map<String, Object> createdFrom = (Map<String, Object>) definition.get(CREATED_FROM);
    List<String> labels = new ArrayList<>();
    labels.add("integration test");

    AlertView alertNoRecipient =
        buildAlertView(
            name,
            AlertType.simple,
            true,
            namespace1.getId(),
            queries,
            threshold,
            notification,
            groupingRules,
            null,
            labels,
            NOT_PASSED);
    Stream<Arguments> withoutRecipients =
        Stream.of(arguments(alertNoRecipient, "without recipients"));

    Stream<Arguments> contacts = ContactIT.buildContacts();
    Stream<Arguments> alertWithDifferentRecipients =
        contacts.map(
            contact -> {
              BatchContact bc = (BatchContact) contact.get()[0];
              String display = "with " + contact.get()[1];

              Map<String, Object> notifctn = new HashMap<>();
              notifctn.put("notification key", "notification value");
              notifctn.put(RECIPIENTS, bc);

              AlertView alertView =
                  buildAlertView(
                      name,
                      AlertType.simple,
                      true,
                      namespace1.getId(),
                      queries,
                      threshold,
                      notifctn,
                      groupingRules,
                      null,
                      labels,
                      1);
              return arguments(alertView, display);
            });

    AlertView withCreatedFrom =
        buildAlertView(
            name,
            AlertType.simple,
            true,
            namespace1.getId(),
            queries,
            threshold,
            notification,
            groupingRules,
            createdFrom,
            labels,
            0);

    AlertView emptyGroupingRules =
        buildAlertView(
            name,
            AlertType.simple,
            true,
            namespace1.getId(),
            queries,
            threshold,
            notification,
            new ArrayList<>(),
            null,
            labels,
            5);

    Stream<Arguments> misc =
        Stream.of(
            arguments(withCreatedFrom, "with created from"),
            arguments(emptyGroupingRules, "with empty grouping rules"));

    Stream<Arguments> stream = Stream.concat(withoutRecipients, alertWithDifferentRecipients);
    return Stream.concat(stream, misc);
  }

  static Map<String, Object> buildDefinition() {
    Map<String, Object> queries = new HashMap<>();
    Map<String, Object> threshold = new HashMap<>();
    Map<String, Object> notification = new HashMap<>();
    List<String> groupingRules = new ArrayList<>();
    Map<String, Object> createdFrom = new HashMap<>();

    queries.put("query key", "query value");
    notification.put("notification key", "notification value");
    threshold.put("threshold key", "threshold value");
    groupingRules.add("grouping rules");
    createdFrom.put("source", "dashboard");

    return buildDefinition(queries, threshold, notification, groupingRules, createdFrom);
  }

  static Map<String, Object> buildDefinition(
      Map<String, Object> queries,
      Map<String, Object> threshold,
      Map<String, Object> notification,
      List<String> groupingRules,
      Map<String, Object> createdFrom) {
    Map<String, Object> definition = new HashMap<>();
    definition.put(QUERIES, queries);
    definition.put(NOTIFICATION, notification);
    definition.put(THRESHOLD, threshold);
    definition.put(GROUPING_RULES, groupingRules);
    definition.put(CREATED_FROM, createdFrom);
    return definition;
  }

  static Alert buildAlert(
      String name,
      AlertType type,
      boolean enabled,
      boolean deleted,
      int namespaceId,
      Map<String, Object> definition,
      List<String> labels,
      String createdBy,
      Timestamp createdTime) {
    Alert alert = new Alert();
    alert.setName(name);
    alert.setType(type);
    alert.setEnabled(enabled);
    alert.setDeleted(deleted);
    alert.setNamespaceId(namespaceId);
    alert.setDefinition(definition);
    alert.setLabels(labels);
    alert.setCreatedBy(createdBy);
    alert.setCreatedTime(createdTime);
    alert.setUpdatedBy(createdBy);
    alert.setUpdatedTime(createdTime);
    return alert;
  }

  private AlertView buildAlertView(
      String name,
      AlertType type,
      boolean enabled,
      int namespaceId,
      Map<String, Object> queries,
      Map<String, Object> threshold,
      Map<String, Object> notification,
      List<String> groupingRules,
      Map<String, Object> createdFrom,
      List<String> labels,
      int version) {
    AlertView alertView = new AlertView();
    alertView.setName(name);
    alertView.setType(type);
    alertView.setEnabled(enabled);
    alertView.setNamespaceId(namespaceId);
    alertView.setQueries(queries);
    alertView.setThreshold(threshold);
    alertView.setNotification(notification);
    alertView.setAlertGroupingRules(groupingRules);
    alertView.setCreatedFrom(createdFrom);
    alertView.setLabels(labels);
    alertView.setVersion(version);
    return alertView;
  }

  private void assertAlertWithoutDefinition(
      BatchContact batchContact, Alert alert, AlertView actual) {
    assertAlertEquals(alert, actual, false);
    BatchContact actualContact = actual.getRecipients();
    assertContactEquals(batchContact, actualContact);
  }

  private void assertAlertWithDefinition(
      Map<String, Object> definition, Alert expected, BatchContact batchContact, AlertView actual) {
    assertAlertEquals(expected, actual, true);
    Map<String, Object> actualNotification = actual.getNotification();
    BatchContact actualContact =
        OBJECT_MAPPER.convertValue(actualNotification.remove(RECIPIENTS), BatchContact.class);
    assertContactEquals(batchContact, actualContact);
    assertEquals(definition.get(NOTIFICATION), actualNotification);
  }

  private void assertAlert(Alert expected, Alert actual) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getName(), actual.getName());
    assertEquals(expected.isEnabled(), actual.isEnabled());
    assertEquals(expected.isDeleted(), actual.isDeleted());
    assertEquals(expected.getNamespaceId(), actual.getNamespaceId());
    assertRecipients(
        OBJECT_MAPPER.convertValue(
            ((Map<String, Object>) expected.getDefinition().get(NOTIFICATION)).get(RECIPIENTS),
            BatchContact.class),
        OBJECT_MAPPER.convertValue(
            ((Map<String, Object>) actual.getDefinition().get(NOTIFICATION)).get(RECIPIENTS),
            BatchContact.class));
    assertEquals(
        expected.getDefinition().get(CREATED_FROM), actual.getDefinition().get(CREATED_FROM));
    assertEquals(expected.getDefinition().get(QUERIES), actual.getDefinition().get(QUERIES));
    assertEquals(expected.getDefinition().get(THRESHOLD), actual.getDefinition().get(THRESHOLD));
    assertEquals(
        expected.getDefinition().get(GROUPING_RULES), actual.getDefinition().get(GROUPING_RULES));
    assertEquals(expected.getDefinition().get(VERSION), actual.getDefinition().get(VERSION));
    assertEquals(expected.getLabels(), actual.getLabels());
  }

  private void assertAlertEquals(Alert expected, AlertView actual, boolean assertBody) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getName(), actual.getName());
    assertEquals(expected.isEnabled(), actual.getEnabled());
    assertEquals(expected.isDeleted(), actual.getDeleted());
    assertEquals(expected.getNamespaceId(), actual.getNamespaceId());

    Map<String, Object> definition = expected.getDefinition();
    assertEquals(expected.getLabels(), actual.getLabels());
    if (assertBody) {
      assertEquals(definition.get(VERSION), actual.getVersion());
      assertEquals(definition.get(QUERIES), actual.getQueries());
      assertEquals(definition.get(THRESHOLD), actual.getThreshold());
      assertEquals(definition.get(GROUPING_RULES), actual.getAlertGroupingRules());
    } else {
      assertEquals(NOT_PASSED, actual.getVersion());
      assertNull(actual.getQueries());
      assertNull(actual.getThreshold());
      assertNull(actual.getNotification());
      assertNull(actual.getAlertGroupingRules());
    }
  }

  private void assertAlertEquals(Alert expected, Alert actual) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.isEnabled(), actual.isEnabled());
    assertEquals(expected.getNamespaceId(), actual.getNamespaceId());

    assertEquals(expected.getCreatedBy(), actual.getCreatedBy());
    assertEquals(expected.getCreatedTime(), actual.getCreatedTime());

    if (expected.isDeleted() ^ actual.isDeleted()) {
      String actualName = actual.getName();
      String expectedName = expected.getName();
      assertTrue(actual.getCreatedTime().getTime() < actual.getUpdatedTime().getTime());
      if (actual.isDeleted()) {
        assertTrue(actualName.startsWith(expectedName) ^ expectedName.startsWith(actualName));
      } else {
        assertEquals(expected.getName(), actual.getName());
      }
    } else {
      assertEquals(expected.getName(), actual.getName());
      assertEquals(expected.getUpdatedBy(), actual.getUpdatedBy());
      assertEquals(expected.getUpdatedTime(), actual.getUpdatedTime());
    }

    Map<String, Object> expectedDefinition = expected.getDefinition();
    Map<String, Object> actualDefinition = actual.getDefinition();
    assertEquals(expectedDefinition.get(QUERIES), actualDefinition.get(QUERIES));
    assertEquals(expectedDefinition.get(THRESHOLD), actualDefinition.get(THRESHOLD));
    assertEquals(expectedDefinition.get(NOTIFICATION), actualDefinition.get(NOTIFICATION));
    assertEquals(expectedDefinition.get(GROUPING_RULES), actualDefinition.get(GROUPING_RULES));
    assertEquals(expectedDefinition.get(CREATED_FROM), actualDefinition.get(CREATED_FROM));
    assertEquals(expectedDefinition.get(VERSION), actualDefinition.get(VERSION));
  }

  private void assertAlertEquals(AlertView expected, Alert actual) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getName(), actual.getName());
    assertEquals(expected.getEnabled(), actual.isEnabled());
    assertEquals(expected.getDeleted(), actual.isDeleted());
    assertEquals(expected.getNamespaceId(), actual.getNamespaceId());
    assertNull(
        ((Map<String, Object>) actual.getDefinition().get(NOTIFICATION))
            .get(RECIPIENTS)); // Recipients are not stored in alert definition
    assertEquals(expected.getCreatedFrom(), actual.getDefinition().get(CREATED_FROM));
    assertEquals(expected.getQueries(), actual.getDefinition().get(QUERIES));
    assertEquals(expected.getThreshold(), actual.getDefinition().get(THRESHOLD));
    assertEquals(expected.getAlertGroupingRules(), actual.getDefinition().get(GROUPING_RULES));
    int expectedVersion = expected.getVersion();
    assertEquals(
        expectedVersion == NOT_PASSED ? AlertService.DEFAULT_VERSION : expectedVersion,
        actual.getDefinition().get(VERSION));
  }

  private void assertAlertEquals(
      List<AlertView> expected, List<AlertView> actual, String createdBy, Timestamp timestamp) {
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      assertAlertEquals(expected.get(i), actual.get(i), createdBy, timestamp);
    }
  }

  private void assertAlertEquals(
      AlertView expected, AlertView actual, String userId, Timestamp timestamp) {
    assertEquals(expected.getName(), actual.getName());
    assertEquals(expected.getEnabled(), actual.getEnabled());
    assertEquals(expected.getDeleted(), actual.getDeleted());
    assertEquals(expected.getNamespaceId(), actual.getNamespaceId());

    Map<String, Object> expectedNotification = expected.getNotification();
    Map<String, Object> actualNotification = actual.getNotification();
    assertRecipients(
        OBJECT_MAPPER.convertValue(expectedNotification.get(RECIPIENTS), BatchContact.class),
        OBJECT_MAPPER.convertValue(actualNotification.get(RECIPIENTS), BatchContact.class));
    Object expectedRecipient = expectedNotification.remove(RECIPIENTS);
    Object actualRecipient = actualNotification.remove(RECIPIENTS);
    assertEquals(expectedNotification, actualNotification);
    expectedNotification.put(RECIPIENTS, expectedRecipient);
    actualNotification.put(RECIPIENTS, actualRecipient);

    assertEquals(expected.getQueries(), actual.getQueries());
    assertEquals(expected.getThreshold(), actual.getThreshold());
    assertEquals(expected.getAlertGroupingRules(), actual.getAlertGroupingRules());
    assertEquals(expected.getCreatedFrom(), actual.getCreatedFrom());
    int expectedVersion = expected.getVersion();
    assertEquals(
        expectedVersion == NOT_PASSED ? AlertService.DEFAULT_VERSION : expectedVersion,
        actual.getVersion());
    if (expected.getId() <= NOT_PASSED) {
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
      assertTrue(timestamp.getTime() <= actual.getUpdatedTime().getTime());
    }
  }

  private void assertRecipients(BatchContact expected, BatchContact actual) {
    if (null != expected && null != actual) {
      BatchContact expectedContact = OBJECT_MAPPER.convertValue(expected, BatchContact.class);
      BatchContact actualContact = OBJECT_MAPPER.convertValue(actual, BatchContact.class);

      ContactIT.assertContactEquals(expectedContact.getEmail(), actualContact.getEmail());
      ContactIT.assertContactEquals(expectedContact.getSlack(), actualContact.getSlack());
      ContactIT.assertContactEquals(expectedContact.getOpsgenie(), actualContact.getOpsgenie());
      ContactIT.assertContactEquals(expectedContact.getHttp(), actualContact.getHttp());
      ContactIT.assertContactEquals(expectedContact.getOc(), actualContact.getOc());
    }
    if (null == expected ^ null == actual) {
      fail("Recipient mis match");
    }
  }
}
