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
import net.opentsdb.horizon.model.Contact;
import net.opentsdb.horizon.model.ContactType;
import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.model.User;
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
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.jayway.restassured.RestAssured.given;
import static net.opentsdb.horizon.converter.BatchContactConverter.APIKEY;
import static net.opentsdb.horizon.converter.BatchContactConverter.CONTEXT;
import static net.opentsdb.horizon.converter.BatchContactConverter.CUSTOMER;
import static net.opentsdb.horizon.converter.BatchContactConverter.DISPLAY_COUNT;
import static net.opentsdb.horizon.converter.BatchContactConverter.EMAIL;
import static net.opentsdb.horizon.converter.BatchContactConverter.ENDPOINT;
import static net.opentsdb.horizon.converter.BatchContactConverter.OPSDB_PROPERTY;
import static net.opentsdb.horizon.converter.BatchContactConverter.WEBHOOK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class ContactIT extends BaseIT {

  private static final String ADMIN_EMAIL_DOMAIN = "@verizonmedia.com";
  private static Timestamp timestamp;
  private Namespace namespace;
  @Override
  protected String getUri() {
    return "namespace";
  }

  @BeforeAll
  public void before() throws IOException {
    timestamp = new Timestamp(System.currentTimeMillis());
    namespace = createNamespace("namesapce1", "test_track", regularMember, timestamp);
    int namespaceId = dbUtil.insert(namespace);
    namespace.setId(namespaceId);
    dbUtil.insertNamespaceMember(namespaceId, regularMember);
  }

  @BeforeEach
  public void beforeMethod() {
    cascadeDeleteContact();
    dbUtil.execute(
        "DELETE FROM namespace_member WHERE userid != '"
            + regularMember
            + "' AND userid != '"
            + unauthorizedMember
            + "'");
    dbUtil.execute(
        "DELETE FROM user WHERE userid != '"
            + regularMember
            + "' AND userid != '"
            + unauthorizedMember
            + "'");
  }

  @ParameterizedTest(name = "[{index}] create {1}")
  @MethodSource("buildContacts")
  void createContact(final BatchContact batchContact, final String displayName) {

    String url = endPoint + "/" + namespace.getName() + "/contact";

    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(batchContact)
            .post(url);
    assertEquals(201, response.getStatusCode());
    BatchContact fromServer = response.as(BatchContact.class);
    assertContactEquals(batchContact, fromServer);

    List<Contact> contactsFromDB = dbUtil.getContactForNamespace(namespace.getId());
    assertContactEquals(fromServer, contactsFromDB, regularMember, timestamp);
  }

  @Test
  public void getContactByNamespace() throws IOException {

    String createdBy = null;
    Stream<Arguments> contactArgs = buildContactModels();
    List<Contact> contacts =
        contactArgs.map(arg -> (Contact) arg.get()[0]).collect(Collectors.toList());
    for (Contact contact : contacts) {
      int id = dbUtil.insert(contact);
      contact.setId(id);
      createdBy = contact.getCreatedBy();
    }

    String url = endPoint + "/" + namespace.getName() + "/contact";
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(url)
            .andReturn();

    assertEquals(200, response.getStatusCode(), response.getBody().asString());
    final BatchContact fromServer = response.getBody().as(BatchContact.class);

    Contact adminContact =
        createAdminEmailContact(regularMember, namespace.getId(), createdBy, timestamp);

    Map<ContactType, List<Contact>> expectedMap =
        contacts.stream().collect(Collectors.groupingBy(c -> c.getType()));
    List<Contact> expectedEmails = expectedMap.get(ContactType.email);
    expectedEmails.add(adminContact); // verify namespace admins are added to email Contacts

    assertContactListEquals(expectedEmails, fromServer.getEmail());
    assertContactListEquals(expectedMap.get(ContactType.slack), fromServer.getSlack());
    assertContactListEquals(expectedMap.get(ContactType.opsgenie), fromServer.getOpsgenie());
    assertContactListEquals(expectedMap.get(ContactType.http), fromServer.getHttp());
    assertContactListEquals(expectedMap.get(ContactType.oc), fromServer.getOc());
  }

  @ParameterizedTest(name = "[{index}] get {1} contacts of a namespace")
  @MethodSource("buildContactModels")
  public void getContactByNamespaceAndType(Contact contact, String displayName) throws IOException {

    int id = dbUtil.insert(contact);
    contact.setId(id);

    String url = endPoint + "/" + namespace.getName() + "/contact?type=" + contact.getType();

    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(url)
            .andReturn();

    assertEquals(200, response.getStatusCode(), response.getBody().asString());

    final BatchContact fromServer = response.getBody().as(BatchContact.class);
    List<Contact> expected = new ArrayList<>();

    expected.add(contact);
    if (contact.getType() == ContactType.email) {
      Contact adminContact =
          createAdminEmailContact(
              regularMember,
              contact.getNamespaceid(),
              contact.getCreatedBy(),
              contact.getCreatedTime());
      expected.add(adminContact);
    }

    assertContactEquals(expected, fromServer);
  }

  @Test
  void getAdminEmailContacts() {

    User u1 = buildUser("user.u1", "User 1", User.CreationMode.onthefly, timestamp);
    User u2 = buildUser("user.u2", "User 2", User.CreationMode.onthefly, timestamp);

    dbUtil.insert(u1);
    dbUtil.insert(u2);
    dbUtil.insertNamespaceMember(namespace.getId(), u1.getUserid());
    dbUtil.insertNamespaceMember(namespace.getId(), u2.getUserid());

    String url = endPoint + "/" + namespace.getName() + "/contact?type=" + ContactType.email;

    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(url)
            .andReturn();
    assertEquals(200, response.getStatusCode());
    List<EmailContact> emailContacts = response.getBody().as(BatchContact.class).getEmail();
    assertEquals(3, emailContacts.size());

    Map<String, EmailContact> contactMap =
        emailContacts.stream()
            .collect(Collectors.toMap(EmailContact::getName, Function.identity()));

    EmailContact e1 = buildAdminEmail(regularMember);
    EmailContact e2 = buildAdminEmail(u1.getUserid());
    EmailContact e3 = buildAdminEmail(u2.getUserid());

    EmailContact a1 = contactMap.get(e1.getEmail());
    EmailContact a2 = contactMap.get(e2.getEmail());
    EmailContact a3 = contactMap.get(e3.getEmail());

    assertContactEquals(e1, a1);
    assertContactEquals(e2, a2);
    assertContactEquals(e3, a3);
  }

  @ParameterizedTest(name = "[{index}] de dupe while getting {1}")
  @MethodSource("urlSourceToGetEmailContacts")
  void deDuplicatesAdminEmailContacts(final String url, final String displayName)
      throws IOException {

    User u1 = buildUser("user.u1", "User 1", User.CreationMode.onthefly, timestamp);
    User u2 = buildUser("user.u2", "User 2", User.CreationMode.onthefly, timestamp);

    dbUtil.insert(u1);
    dbUtil.insert(u2);
    dbUtil.insertNamespaceMember(namespace.getId(), u1.getUserid());

    Contact c1 =
        createEmailContact(
            buildAdminEmailId(u1.getUserid()), namespace.getId(), u1.getUserid(), timestamp);
    Contact c2 =
        createEmailContact(
            buildAdminEmailId(u2.getUserid()), namespace.getId(), u2.getUserid(), timestamp);

    int id = dbUtil.insert(c1); // admin
    c1.setId(id);

    id = dbUtil.insert(c2); // not admin
    c2.setId(id);

    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(url)
            .andReturn();
    assertEquals(200, response.getStatusCode());
    List<EmailContact> actualList = response.getBody().as(BatchContact.class).getEmail();
    assertEquals(3, actualList.size());

    Map<String, EmailContact> actualMap =
        actualList.stream().collect(Collectors.toMap(EmailContact::getName, Function.identity()));

    Contact c3 =
        createEmailContact(
            buildAdminEmailId(regularMember),
            namespace.getId(),
            regularMember,
            timestamp); // namespace member, not added to the namespace contacts

    EmailContact a1 = actualMap.get(c1.getName());
    EmailContact a2 = actualMap.get(c2.getName());
    EmailContact a3 = actualMap.get(c3.getName());

    assertTrue(a1.isAdmin());
    assertFalse(a2.isAdmin());
    assertTrue(a3.isAdmin());

    assertContactEquals(c1, a1);
    assertContactEquals(c2, a2);
    assertContactEquals(c3, a3);
  }

  @ParameterizedTest(name = "[{index}] update {2}")
  @MethodSource("buildContactsToUpdate")
  public void updateContact(Contact original, BatchContact modified, String displayName)
      throws IOException {

    int id = dbUtil.insert(original);
    original.setId(id);

    BaseContact modifiedContact;

    if (original.getType().equals(ContactType.email)) {
      modifiedContact = modified.getEmail().get(0);
    } else if (original.getType().equals(ContactType.slack)) {
      modifiedContact = modified.getSlack().get(0);
    } else if (original.getType().equals(ContactType.opsgenie)) {
      modifiedContact = modified.getOpsgenie().get(0);
    } else if (original.getType().equals(ContactType.http)) {
      modifiedContact = modified.getHttp().get(0);
    } else {
      modifiedContact = modified.getOc().get(0);
    }

    modifiedContact.setId(id);

    String url = endPoint + "/" + namespace.getName() + "/contact";
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(modified)
            .put(url)
            .thenReturn();
    assertEquals(200, response.getStatusCode());

    BatchContact fromServer = response.as(BatchContact.class);
    assertContactEquals(modified, fromServer);

    Contact fromDB = dbUtil.getContactById(id).get();

    assertContactEquals(modifiedContact, fromDB);

    assertEquals(original.getCreatedBy(), fromDB.getCreatedBy());
    assertEquals(original.getCreatedTime(), fromDB.getCreatedTime());
    assertEquals(regularMember, fromDB.getUpdatedBy());
    assertTrue(original.getUpdatedTime().getTime() < fromDB.getUpdatedTime().getTime());
  }

  @Test
  void deleteContact() throws IOException {
    String createdBy = "user.testuser";

    String emailId = "test@vz.com";
    Contact email = createEmailContact(emailId, namespace.getId(), createdBy, timestamp);

    EmailContact emailContact = buildEmailContact(emailId, false);
    emailContact.setEmail(email.getDetails().get(EMAIL));

    String slackName = "test slack";
    String webHook = "test hook";
    Contact slack = createSlackContact(slackName, webHook, namespace.getId(), createdBy, timestamp);
    SlackContact slackContact = new SlackContact();
    slackContact.setName(slackName);
    slackContact.setWebhook(webHook);

    String opsGenieName = "test opsgenie";
    String apiKey = "test key";
    Contact opsGenie =
        createOpsGenieContact(opsGenieName, apiKey, namespace.getId(), createdBy, timestamp);
    OpsGenieContact opsGenieContact = new OpsGenieContact();
    opsGenieContact.setName(opsGenieName);
    opsGenieContact.setApikey(apiKey);

    String httpName = "test http";
    String endpoint = "test endpoint";
    Contact http = createHttpContact(httpName, endpoint, namespace.getId(), createdBy, timestamp);
    HttpContact httpContact = new HttpContact();
    httpContact.setName(httpName);
    httpContact.setEndpoint(endpoint);
    BatchContact batchContact6 = new BatchContact();
    batchContact6.setHttp(Arrays.asList(httpContact));

    String ocName = "test oc";
    String context = "test context";
    String customer = "test customer";
    String displayCount = "test displaycount";
    String opsdbProperty = "test opsdbproperty";
    Contact oc =
        createOcContact(
            ocName,
            context,
            customer,
            displayCount,
            opsdbProperty,
            namespace.getId(),
            createdBy,
            timestamp);
    OCContact ocContact = new OCContact();
    ocContact.setName(ocName);
    ocContact.setContext(context);
    ocContact.setCustomer(customer);
    ocContact.setDisplaycount(displayCount);
    ocContact.setOpsdbproperty(opsdbProperty);

    int id = dbUtil.insert(email);
    email.setId(id);
    emailContact.setId(id);

    id = dbUtil.insert(slack);
    slack.setId(id);
    slackContact.setId(id);

    id = dbUtil.insert(opsGenie);
    opsGenie.setId(id);
    opsGenieContact.setId(id);

    id = dbUtil.insert(http);
    http.setId(id);
    httpContact.setId(id);

    id = dbUtil.insert(oc);
    oc.setId(id);
    ocContact.setId(id);

    BatchContact batchContact = new BatchContact();
    batchContact.setEmail(Arrays.asList(emailContact));
    batchContact.setOc(Arrays.asList(ocContact));

    String url = endPoint + "/" + namespace.getName() + "/contact/delete";
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(batchContact)
            .put(url)
            .thenReturn();
    assertEquals(200, response.getStatusCode());

    assertFalse(dbUtil.getContactById(emailContact.getId()).isPresent());
    assertFalse(dbUtil.getContactById(ocContact.getId()).isPresent());
    assertContactEquals(slack, dbUtil.getContactById(slack.getId()).get());
    assertContactEquals(opsGenie, dbUtil.getContactById(opsGenie.getId()).get());
    assertContactEquals(http, dbUtil.getContactById(http.getId()).get());

    // delete without id
    batchContact = new BatchContact();
    slackContact.setId(0);
    batchContact.setSlack(Arrays.asList(slackContact));
    opsGenieContact.setId(0);
    batchContact.setOpsgenie(Arrays.asList(opsGenieContact));

    response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(batchContact)
            .put(url)
            .thenReturn();
    assertEquals(200, response.getStatusCode());

    assertFalse(dbUtil.getContactById(emailContact.getId()).isPresent());
    assertFalse(dbUtil.getContactById(ocContact.getId()).isPresent());
    assertContactEquals(http, dbUtil.getContactById(http.getId()).get());
  }

  private Stream<Arguments> urlSourceToGetEmailContacts() {
    String getByNamespace = endPoint + "/" + namespace.getName() + "/contact";
    String getByNamespaceAndType =
        endPoint + "/" + namespace.getName() + "/contact?type=" + ContactType.email;
    return Stream.of(
        arguments(getByNamespace, "all contacts for a namespace"),
        arguments(getByNamespaceAndType, "email contacts for a namespace"));
  }

  static Stream<Arguments> buildContacts() {
    String emailId = "test@vz.com";
    EmailContact email1 = buildEmailContact(emailId, false);
    BatchContact batchContact1 = new BatchContact();
    batchContact1.setEmail(Arrays.asList(email1));

    EmailContact email2 = buildEmailContact("test admin", false);
    email2.setEmail("test@vz.com");
    BatchContact batchContact2 = new BatchContact();
    batchContact2.setEmail(Arrays.asList(email2));

    SlackContact slack = new SlackContact();
    slack.setName("test slack");
    slack.setWebhook("test hook");
    BatchContact batchContact3 = new BatchContact();
    batchContact3.setSlack(Arrays.asList(slack));

    OpsGenieContact opsGenie = new OpsGenieContact();
    opsGenie.setName("test opsgenie");
    opsGenie.setApikey("test key");
    BatchContact batchContact4 = new BatchContact();
    batchContact4.setOpsgenie(Arrays.asList(opsGenie));

    HttpContact httpContact = new HttpContact();
    httpContact.setName("test http");
    httpContact.setEndpoint("test endpoint");
    BatchContact batchContact5 = new BatchContact();
    batchContact5.setHttp(Arrays.asList(httpContact));

    OCContact ocContact = new OCContact();
    ocContact.setName("test oc");
    ocContact.setContext("test context");
    ocContact.setCustomer("test customer");
    ocContact.setDisplaycount("test displaycount");
    ocContact.setOpsdbproperty("test opsdbproperty");
    BatchContact batchContact6 = new BatchContact();
    batchContact6.setOc(Arrays.asList(ocContact));

    BatchContact batchContact7 = new BatchContact();
    batchContact7.setEmail(Arrays.asList(email2));
    batchContact7.setSlack(Arrays.asList(slack));
    batchContact7.setOpsgenie(Arrays.asList(opsGenie));
    batchContact7.setHttp(Arrays.asList(httpContact));
    batchContact7.setOc(Arrays.asList(ocContact));

    return Stream.of(
        arguments(batchContact1, "Email contact"),
        arguments(batchContact2, "Email contact with name"),
        arguments(batchContact3, "Slack contact"),
        arguments(batchContact4, "OpsGenie contact"),
        arguments(batchContact5, "Http contact"),
        arguments(batchContact6, "OC contact"),
        arguments(batchContact6, "All types of contacts"));
  }

  private Stream<Arguments> buildContactModels() {
    String createdBy = "user.testuser";
    Timestamp createdTime = timestamp;

    Contact email = createEmailContact("test@vz.com", namespace.getId(), createdBy, createdTime);

    String slackName = "test slack";
    String webHook = "test hook";
    Contact slack =
        createSlackContact(slackName, webHook, namespace.getId(), createdBy, createdTime);

    String opsGenieName = "test opsgenie";
    String apiKey = "test key";
    Contact opsGenie =
        createOpsGenieContact(opsGenieName, apiKey, namespace.getId(), createdBy, createdTime);

    String httpName = "test http";
    String endpoint = "test endpoint";
    Contact http = createHttpContact(httpName, endpoint, namespace.getId(), createdBy, createdTime);

    String ocName = "test oc";
    String context = "test context";
    String customer = "test customer";
    String displayCount = "test displaycount";
    String opsdbProperty = "test opsdbproperty";
    Contact oc =
        createOcContact(
            ocName,
            context,
            customer,
            displayCount,
            opsdbProperty,
            namespace.getId(),
            createdBy,
            createdTime);
    return Stream.of(
        arguments(email, "email"),
        arguments(slack, "slack"),
        arguments(opsGenie, "opsGenie"),
        arguments(http, "http"),
        arguments(oc, "oc"));
  }

  private Stream<Arguments> buildContactsToUpdate() {

    Stream<Arguments> contactArgs = buildContactModels();
    List<Contact> contacts =
        contactArgs.map(arg -> (Contact) arg.get()[0]).collect(Collectors.toList());

    Contact email = contacts.get(0);
    EmailContact emailContact = buildEmailContact(email.getDetails().get(EMAIL), false);
    emailContact.setName("test email contact");
    BatchContact batchContact1 = new BatchContact();
    batchContact1.setEmail(Arrays.asList(emailContact));

    Contact slack = contacts.get(1);
    SlackContact slack1 = new SlackContact();
    slack1.setName("new " + slack.getName());
    slack1.setWebhook(slack.getDetails().get(WEBHOOK));
    BatchContact batchContact2 = new BatchContact();
    batchContact2.setSlack(Arrays.asList(slack1));

    SlackContact slack2 = new SlackContact();
    slack2.setName(slack.getName());
    slack2.setWebhook("new " + slack.getDetails().get(WEBHOOK));
    BatchContact batchContact3 = new BatchContact();
    batchContact3.setSlack(Arrays.asList(slack2));

    Contact opsGenie = contacts.get(2);
    OpsGenieContact opsGenie1 = new OpsGenieContact();
    opsGenie1.setName("new " + opsGenie.getName());
    opsGenie1.setApikey(opsGenie.getDetails().get(APIKEY));
    BatchContact batchContact4 = new BatchContact();
    batchContact4.setOpsgenie(Arrays.asList(opsGenie1));

    OpsGenieContact opsGenie2 = new OpsGenieContact();
    opsGenie2.setName(opsGenie.getName());
    opsGenie2.setApikey("new " + opsGenie.getDetails().get(APIKEY));
    BatchContact batchContact5 = new BatchContact();
    batchContact5.setOpsgenie(Arrays.asList(opsGenie2));

    Contact http = contacts.get(3);
    HttpContact http1 = new HttpContact();
    http1.setName("new " + http.getName());
    http1.setEndpoint(http.getDetails().get(ENDPOINT));
    BatchContact batchContact6 = new BatchContact();
    batchContact6.setHttp(Arrays.asList(http1));

    HttpContact http2 = new HttpContact();
    http2.setName(http.getName());
    http2.setEndpoint("new " + http.getDetails().get(ENDPOINT));
    BatchContact batchContact7 = new BatchContact();
    batchContact7.setHttp(Arrays.asList(http2));

    Contact oc = contacts.get(4);
    Map<String, String> ocDetails = oc.getDetails();
    OCContact ocContact1 = new OCContact();
    ocContact1.setName("new " + oc.getName());
    ocContact1.setContext(ocDetails.get(CONTEXT));
    ocContact1.setCustomer(ocDetails.get(CUSTOMER));
    ocContact1.setDisplaycount(ocDetails.get(DISPLAY_COUNT));
    ocContact1.setOpsdbproperty(ocDetails.get(OPSDB_PROPERTY));
    BatchContact batchContact8 = new BatchContact();
    batchContact8.setOc(Arrays.asList(ocContact1));

    OCContact ocContact2 = new OCContact();
    ocContact2.setName(oc.getName());
    ocContact2.setContext("new" + ocDetails.get(CONTEXT));
    ocContact2.setCustomer(ocDetails.get(CUSTOMER));
    ocContact2.setDisplaycount(ocDetails.get(DISPLAY_COUNT));
    ocContact2.setOpsdbproperty(ocDetails.get(OPSDB_PROPERTY));
    BatchContact batchContact9 = new BatchContact();
    batchContact9.setOc(Arrays.asList(ocContact2));

    return Stream.of(
        arguments(email, batchContact1, "Email name"),
        arguments(slack, batchContact2, "Slack name"),
        arguments(slack, batchContact3, "Slack web hook"),
        arguments(opsGenie, batchContact4, "OpsGenie name"),
        arguments(opsGenie, batchContact5, "OpsGenie api key"),
        arguments(http, batchContact6, "Http name"),
        arguments(http, batchContact7, "Http endpoint"),
        arguments(oc, batchContact8, "Oc name"),
        arguments(oc, batchContact9, "Oc context"));
  }

  static EmailContact buildEmailContact(String emailId, boolean isAdmin) {
    EmailContact email = new EmailContact();
    email.setEmail(emailId);
    email.setAdmin(isAdmin);
    return email;
  }

  static SlackContact buildSlackContact(String name, String webHook) {
    SlackContact slackContact = new SlackContact();
    slackContact.setName(name);
    slackContact.setWebhook(webHook);
    return slackContact;
  }

  static OpsGenieContact buildOpsGenieContact(String name, String apikey) {
    OpsGenieContact opsGenieContact = new OpsGenieContact();
    opsGenieContact.setName(name);
    opsGenieContact.setApikey(apikey);
    return opsGenieContact;
  }

  static HttpContact buildHttpContact(String name, String endPoint) {
    HttpContact httpContact = new HttpContact();
    httpContact.setName(name);
    httpContact.setEndpoint(endPoint);
    return httpContact;
  }

  static OCContact buildOCContact(
      String name, String context, String customer, String opsdbProperty, String displayCount) {
    OCContact ocContact = new OCContact();
    ocContact.setName(name);
    ocContact.setContext(context);
    ocContact.setCustomer(customer);
    ocContact.setOpsdbproperty(opsdbProperty);
    ocContact.setDisplaycount(displayCount);
    return ocContact;
  }

  private Contact createOcContact(
      String name,
      String context,
      String customer,
      String displayCount,
      String opsdbProperty,
      int namespaceId,
      String createdBy,
      Timestamp timestamp) {
    Map<String, String> details = new HashMap<>();
    details.put(CONTEXT, context);
    details.put(CUSTOMER, customer);
    details.put(DISPLAY_COUNT, displayCount);
    details.put(OPSDB_PROPERTY, opsdbProperty);
    return createContact(name, ContactType.oc, details, namespaceId, createdBy, timestamp);
  }

  private Contact createHttpContact(
      String name, String endpoint, int namespaceId, String createdBy, Timestamp createdTime) {
    Map<String, String> details = new HashMap<>();
    details.put(ENDPOINT, endpoint);
    return createContact(name, ContactType.http, details, namespaceId, createdBy, createdTime);
  }

  private Contact createOpsGenieContact(
      String name, String apiKey, int namespaceId, String createdBy, Timestamp createdTime) {
    Map<String, String> details = new HashMap<>();
    details.put(APIKEY, apiKey);
    return createContact(name, ContactType.opsgenie, details, namespaceId, createdBy, createdTime);
  }

  private Contact createSlackContact(
      String name, String webHook, int namespaceId, String createdBy, Timestamp createdTime) {
    Map<String, String> details = new HashMap<>();
    details.put(WEBHOOK, webHook);
    return createContact(name, ContactType.slack, details, namespaceId, createdBy, createdTime);
  }

  private Contact createAdminEmailContact(
      String userId, int namespaceId, String createdBy, Timestamp timestamp) {
    String emailId = buildAdminEmailId(userId);
    return createEmailContact(emailId, namespaceId, createdBy, timestamp);
  }

  private String buildAdminEmailId(String userId) {
    String name = userId.substring(userId.indexOf(".") + 1);
    return name + ADMIN_EMAIL_DOMAIN;
  }

  private EmailContact buildAdminEmail(final String userId) {
    String emailId = buildAdminEmailId(userId);
    return buildEmailContact(emailId, true);
  }

  private Contact createEmailContact(
      String emailId, int namespaceId, String createdBy, Timestamp createdTime) {

    Map<String, String> details = new HashMap<>();
    details.put(EMAIL, emailId);
    return createContact(emailId, ContactType.email, details, namespaceId, createdBy, createdTime);
  }

  static <T extends BaseContact> Contact toModel(
      T t, int namespaceId, String createdBy, Timestamp createdTime) {

    Map<String, String> details = new HashMap<>();
    ContactType type;

    if (t instanceof EmailContact) {
      type = ContactType.email;
      EmailContact email = (EmailContact) t;
      details.put(EMAIL, email.getEmail());
      if (Utils.isNullOrEmpty(t.getName())) {
        t.setName(email.getEmail());
      }
    } else if (t instanceof SlackContact) {
      type = ContactType.slack;
      SlackContact slack = (SlackContact) t;
      details.put(WEBHOOK, slack.getWebhook());
    } else if (t instanceof OpsGenieContact) {
      type = ContactType.opsgenie;
      OpsGenieContact opsGenie = (OpsGenieContact) t;
      details.put(APIKEY, opsGenie.getApikey());
    } else if (t instanceof HttpContact) {
      type = ContactType.http;
      HttpContact http = (HttpContact) t;
      details.put(ENDPOINT, http.getEndpoint());
    } else if (t instanceof OCContact) {
      type = ContactType.oc;
      OCContact oc = (OCContact) t;
      details.put(CONTEXT, oc.getContext());
      details.put(CUSTOMER, oc.getCustomer());
      details.put(DISPLAY_COUNT, oc.getDisplaycount());
      details.put(OPSDB_PROPERTY, oc.getOpsdbproperty());
    } else {
      throw new IllegalArgumentException("Invalid contact type: " + t.getClass());
    }
    return createContact(t.getName(), type, details, namespaceId, createdBy, createdTime);
  }

  static Contact createContact(
      String name,
      ContactType type,
      Map<String, String> details,
      int namespaceId,
      String createdBy,
      Timestamp createdTime) {
    Contact contact = new Contact();
    contact.setName(name);
    contact.setType(type);
    contact.setDetails(details);
    contact.setNamespaceid(namespaceId);
    contact.setCreatedBy(createdBy);
    contact.setCreatedTime(createdTime);
    contact.setUpdatedBy(createdBy);
    contact.setUpdatedTime(createdTime);
    return contact;
  }

  private void assertContactEquals(List<Contact> expected, BatchContact actual) {
    Map<ContactType, List<Contact>> expectedMap =
        expected.stream().collect(Collectors.groupingBy(c -> c.getType()));
    assertContactListEquals(expectedMap.get(ContactType.email), actual.getEmail());
    assertContactListEquals(expectedMap.get(ContactType.slack), actual.getSlack());
    assertContactListEquals(expectedMap.get(ContactType.opsgenie), actual.getOpsgenie());
    assertContactListEquals(expectedMap.get(ContactType.http), actual.getHttp());
    assertContactListEquals(expectedMap.get(ContactType.oc), actual.getOc());
  }

  static void assertContactEquals(BatchContact expected, List<Contact> actual, String userId, Timestamp timestamp) {
    Map<ContactType, List<Contact>> actualMap =
        actual.stream().collect(Collectors.groupingBy(c -> c.getType()));
    assertContactListEquals(expected.getEmail(), actualMap.get(ContactType.email), userId, timestamp);
    assertContactListEquals(expected.getSlack(), actualMap.get(ContactType.slack), userId, timestamp);
    assertContactListEquals(expected.getOpsgenie(), actualMap.get(ContactType.opsgenie), userId, timestamp);
    assertContactListEquals(expected.getHttp(), actualMap.get(ContactType.http), userId, timestamp);
    assertContactListEquals(expected.getOc(), actualMap.get(ContactType.oc), userId, timestamp);
  }

  private <T extends BaseContact> void assertContactListEquals(
      List<Contact> expected, List<T> actual) {
    if (expected == null) {
      assertNull(actual);
    } else {
      assertEquals(expected.size(), actual.size());
      for (int i = 0; i < expected.size(); i++) {
        assertContactEquals(expected.get(i), actual.get(i));
      }
    }
  }

  private static <T extends BaseContact> void assertContactListEquals(
      List<T> expected, List<Contact> actual, String userId, Timestamp timestamp) {
    if (expected == null) {
      assertNull(actual);
    } else {
      assertEquals(expected.size(), actual.size());
      for (int i = 0; i < expected.size(); i++) {
        T e = expected.get(i);
        Contact a = actual.get(i);

        if (e.getId() == 0) {
          assertTrue(a.getId() > 0); // assert for new contact
        } else {
          assertEquals(e.getId(), a.getId()); // assert for update contact
        }

        assertContactEquals(e, a);

        assertEquals(userId, a.getCreatedBy());
        assertEquals(userId, a.getUpdatedBy());
        Timestamp createdTime = a.getCreatedTime();
        assertTrue(createdTime.getTime() >= timestamp.getTime());
        assertEquals(createdTime, a.getUpdatedTime());
      }
    }
  }

  private void assertContactEquals(Contact expected, Contact actual) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getType(), actual.getType());
    assertEquals(expected.getDetails(), actual.getDetails());
    assertEquals(expected.getNamespaceid(), actual.getNamespaceid());
    assertEquals(expected.getCreatedBy(), actual.getCreatedBy());
    assertEquals(expected.getCreatedTime(), actual.getCreatedTime());
    assertEquals(expected.getUpdatedBy(), actual.getUpdatedBy());
    assertEquals(expected.getUpdatedTime(), actual.getUpdatedTime());
  }

  private static void assertContactEquals(BaseContact expected, Contact actual) {
    if (expected instanceof EmailContact) {
      assertContactEquals((EmailContact) expected, actual);
    } else if (expected instanceof SlackContact) {
      assertContactEquals((SlackContact) expected, actual);
    } else if (expected instanceof OpsGenieContact) {
      assertContactEquals((OpsGenieContact) expected, actual);
    } else if (expected instanceof HttpContact) {
      assertContactEquals((HttpContact) expected, actual);
    } else {
      assertContactEquals((OCContact) expected, actual);
    }
  }

  private void assertContactEquals(Contact expected, BaseContact actual) {
    ContactType type = expected.getType();
    if (type == ContactType.email) {
      assertContactEquals(expected, (EmailContact) actual);
    } else if (type == ContactType.slack) {
      assertContactEquals(expected, (SlackContact) actual);
    } else if (type == ContactType.opsgenie) {
      assertContactEquals(expected, (OpsGenieContact) actual);
    } else if (type == ContactType.http) {
      assertContactEquals(expected, (HttpContact) actual);
    } else {
      assertContactEquals(expected, (OCContact) actual);
    }
  }

  private static void assertContactEquals(EmailContact expected, Contact actual) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getName(), actual.getName());
    assertEquals(ContactType.email, actual.getType());
    Map<String, String> details = actual.getDetails();
    assertEquals(expected.getEmail(), details.get(EMAIL));
  }

  private static void assertContactEquals(Contact expected, EmailContact actual) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getName(), actual.getName());
    Map<String, String> details = expected.getDetails();
    assertEquals(details.get(EMAIL), actual.getEmail());
  }

  private static void assertContactEquals(SlackContact expected, Contact actual) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getName(), actual.getName());
    assertEquals(ContactType.slack, actual.getType());
    Map<String, String> details = actual.getDetails();
    assertEquals(expected.getWebhook(), details.get(WEBHOOK));
  }

  private void assertContactEquals(Contact expected, SlackContact actual) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getName(), actual.getName());
    assertEquals(expected.getDetails().get(WEBHOOK), actual.getWebhook());
  }

  private static void assertContactEquals(OpsGenieContact expected, Contact actual) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getName(), actual.getName());
    assertEquals(ContactType.opsgenie, actual.getType());
    Map<String, String> details = actual.getDetails();
    assertEquals(expected.getApikey(), details.get(APIKEY));
  }

  private void assertContactEquals(Contact expected, OpsGenieContact actual) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getName(), actual.getName());
    Map<String, String> details = expected.getDetails();
    assertEquals(details.get(APIKEY), actual.getApikey());
  }

  private static void assertContactEquals(HttpContact expected, Contact actual) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getName(), actual.getName());
    assertEquals(ContactType.http, actual.getType());
    Map<String, String> details = actual.getDetails();
    assertEquals(expected.getEndpoint(), details.get(ENDPOINT));
  }

  private void assertContactEquals(Contact expected, HttpContact actual) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getName(), actual.getName());
    Map<String, String> details = expected.getDetails();
    assertEquals(details.get(ENDPOINT), actual.getEndpoint());
  }

  private static void assertContactEquals(OCContact expected, Contact actual) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getName(), actual.getName());
    assertEquals(ContactType.oc, actual.getType());
    Map<String, String> details = actual.getDetails();
    assertEquals(expected.getDisplaycount(), details.get(DISPLAY_COUNT));
    assertEquals(expected.getContext(), details.get(CONTEXT));
    assertEquals(expected.getCustomer(), details.get(CUSTOMER));
    assertEquals(expected.getOpsdbproperty(), details.get(OPSDB_PROPERTY));
  }

  private void assertContactEquals(Contact expected, OCContact actual) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getName(), actual.getName());
    Map<String, String> details = expected.getDetails();
    assertEquals(details.get(DISPLAY_COUNT), actual.getDisplaycount());
    assertEquals(details.get(CONTEXT), actual.getContext());
    assertEquals(details.get(CUSTOMER), actual.getCustomer());
    assertEquals(details.get(OPSDB_PROPERTY), actual.getOpsdbproperty());
  }

  static void assertContactEquals(BatchContact expected, BatchContact actual) {
    assertContactEquals(expected.getEmail(), actual.getEmail());
    assertContactEquals(expected.getSlack(), actual.getSlack());
    assertContactEquals(expected.getOpsgenie(), actual.getOpsgenie());
    assertContactEquals(expected.getHttp(), actual.getHttp());
    assertContactEquals(expected.getOc(), actual.getOc());
  }

  static <T extends BaseContact> void assertContactEquals(List<T> expected, List<T> actual) {
    if (expected == null) {
      assertNull(actual);
    } else {
      assertEquals(expected.size(), actual.size());
      for (int i = 0; i < expected.size(); i++) {
        T e = expected.get(i);
        T a = actual.get(i);

        if (e.getId() == 0) {
          assertTrue(a.getId() > 0); // assert for new contact
        } else {
          assertEquals(e.getId(), a.getId()); // assert for update contact
        }

        if (e instanceof EmailContact) {
          assertContactEquals((EmailContact) e, (EmailContact) a);
        } else if (e instanceof SlackContact) {
          assertContactEquals((SlackContact) e, (SlackContact) a);
        } else if (e instanceof OpsGenieContact) {
          assertContactEquals((OpsGenieContact) e, (OpsGenieContact) a);
        } else if (e instanceof HttpContact) {
          assertContactEquals((HttpContact) e, (HttpContact) a);
        } else {
          assertContactEquals((OCContact) e, (OCContact) a);
        }
      }
    }
  }

  private static void assertContactEquals(EmailContact expected, EmailContact actual) {
    if (expected.getName() == null) {
      assertEquals(expected.getEmail(), actual.getName());
    } else {
      assertEquals(expected.getName(), actual.getName());
    }
    assertEquals(expected.getEmail(), actual.getEmail());
    assertEquals(expected.isAdmin(), actual.isAdmin());
  }

  private static void assertContactEquals(SlackContact expected, SlackContact actual) {
    assertBaseContactEquals(expected, actual);
    assertEquals(expected.getWebhook(), actual.getWebhook());
  }

  private static void assertContactEquals(OpsGenieContact expected, OpsGenieContact actual) {
    assertBaseContactEquals(expected, actual);
    assertEquals(expected.getApikey(), actual.getApikey());
  }

  private static void assertContactEquals(HttpContact expected, HttpContact actual) {
    assertBaseContactEquals(expected, actual);
    assertEquals(expected.getEndpoint(), actual.getEndpoint());
  }

  private static void assertContactEquals(OCContact expected, OCContact actual) {
    assertBaseContactEquals(expected, actual);
    assertEquals(expected.getDisplaycount(), actual.getDisplaycount());
    assertEquals(expected.getContext(), actual.getContext());
    assertEquals(expected.getCustomer(), actual.getCustomer());
    assertEquals(expected.getOpsdbproperty(), actual.getOpsdbproperty());
  }

  private static void assertBaseContactEquals(BaseContact expected, BaseContact actual) {
    assertEquals(expected.getName(), actual.getName());
  }
}
