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
import net.opentsdb.horizon.model.User;
import net.opentsdb.horizon.service.NamespaceService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.jayway.restassured.RestAssured.given;
import static net.opentsdb.horizon.util.Utils.slugify;
import static javax.ws.rs.core.Response.Status.CREATED;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class NamespaceIT extends BaseIT {

    @Override
    protected String getUri() {
        return "namespace";
    }

    @BeforeEach
    public void beforeMethod() {
        cascadeDeleteNamespace();
    }

    @Test
    public void getNamespaceByNamespaceId() throws IOException {
        long now = System.currentTimeMillis();
        Namespace namespace = getNamespace();
        final Integer namespaceId = createNamespaceWithMember(namespace, regularMember);

        String url = endPoint + "/" + namespaceId; // id are auto generated. get id from database.

        Response response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON).header(cookieHeader).get(url).andReturn();

        assertEquals(200, response.getStatusCode(), response.getBody().asString());
        final Namespace createdNamespace = response.getBody().as(Namespace.class);

        // confirm json response
        assertNamespaceResponse(now, namespace, createdNamespace, namespace.getName(), regularMember, regularMember);
    }

    @Test
    public void getNamespaceByAlias() throws IOException {
        long now = System.currentTimeMillis();
        Namespace namespace = getNamespace();
        createNamespaceWithMember(namespace, regularMember);

        String url = endPoint + "/?alias=" + namespace.getAlias();

        Response response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON).header(cookieHeader).get(url).andReturn();

        assertEquals(200, response.getStatusCode(), response.getBody().asString());
        final Namespace createdNamespace = response.getBody().as(Namespace.class);
        assertNotNull(createdNamespace);
        // confirm json response
        assertNamespaceResponse(now, namespace, createdNamespace, namespace.getName(), regularMember, regularMember);
    }

    @Test
    public void getNamespaceByName() throws IOException {
        long now = System.currentTimeMillis();
        Namespace namespace = getNamespace();
        createNamespaceWithMember(namespace, regularMember);

        String url = endPoint + "/?name=" + namespace.getName();

        Response response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON).header(cookieHeader).get(url).andReturn();

        assertEquals(200, response.getStatusCode(), response.getBody().asString());
        final Namespace createdNamespace = response.getBody().as(Namespace.class);
        // confirm json response
        assertNamespaceResponse(now, namespace, createdNamespace, namespace.getName(), regularMember, regularMember);
    }

    @ParameterizedTest
    @MethodSource("testDataToFetchByNameOrAlias")
    public void getNamespaceByNameOrAliasOption(String queryparam, String nameOrAlias, int expectedcount, int statuscode) throws IOException {
        long now = System.currentTimeMillis();
        Namespace namespace = getNamespace();
        createNamespaceWithMember(namespace, regularMember);

        String url = endPoint + "/" + queryparam + nameOrAlias;

        Response response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON).header(cookieHeader).get(url).andReturn();
        assertEquals(response.getStatusCode(), statuscode);

        if (expectedcount > 0) {
            final Namespace createdNamespace = response.getBody().as(Namespace.class);
            assertNotNull(createdNamespace);
            // confirm json response
            assertNamespaceResponse(now, namespace, createdNamespace, namespace.getName(), regularMember, regularMember);
        }
    }

    private static Stream<Arguments> testDataToFetchByNameOrAlias() {
        return Stream.of(
            arguments("?includeNameOrAlias=true&name=", "OpenTSDBAlias", 1, 200),
            arguments("?includeNameOrAlias=true&alias=", "OpenTSDB", 1, 200),
            // UnknownAlias does not match with any name or alias
            arguments("?includeNameOrAlias=true&name=", "UnknownAlias", 0, 404),
            // UnknownAlias does not match with any name or alias
            arguments("?includeNameOrAlias=true&alias=", "UnknownAlias", 0, 404)
        );
    }

    @Test
    public void getNamespaceShouldReturn404WhenIdNotFound() {
        String url = endPoint + "/" + 1; // when id not available should return 404
        Response response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON).header(cookieHeader).get(url).andReturn();
        assertEquals(404, response.getStatusCode());
    }

    @Test
    public void getNamespaceShouldReturn404WhenNameNotFound() {
        String url = endPoint + "/?name=notavailablename";
        Response response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON).header(cookieHeader).get(url).andReturn();
        assertEquals(404, response.getStatusCode());
    }

    @Test
    public void getNamespaceShouldReturn404WhenAliasNotFound() {
        String url = endPoint + "/?alias=notavailablealias";
        Response response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON).header(cookieHeader).get(url).andReturn();
        assertEquals(404, response.getStatusCode());
    }

    @Test
    public void createNamespace() {
        long now = System.currentTimeMillis();
        Namespace namespace = getNamespace();
        Response response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON).header(cookieHeader).body(namespace).post(endPoint);

        assertEquals(201, response.getStatusCode(), response.getBody().asString());
        Namespace createdNamespace = response.as(Namespace.class);

        // confirm json response
        assertNamespaceResponse(now, namespace, createdNamespace, namespace.getName(), regularMember, regularMember);

        // confirm with db entry
        Namespace actualNamespace = dbUtil.getNamespaceByName(namespace.getName());
        assertStoredNamespace(now, createdNamespace, actualNamespace);
    }

    @ParameterizedTest
    @MethodSource("testNamespaceCreationWhenNameOrAliasExist")
    public void shouldNotCreateNamespaceWhenNameOrAliasExist(String name, String alias) throws IOException {
        // create namespace already
        Namespace storedNamespace = getNamespace();
        createNamespaceWithMember(storedNamespace, regularMember);

        final Namespace namespace = getNamespace();
        namespace.setName(name);
        namespace.setAlias(alias);
        Response response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON).header(cookieHeader).body(namespace).post(endPoint);
        assertEquals(400, response.getStatusCode());
    }

    private static Stream<Arguments> testNamespaceCreationWhenNameOrAliasExist() {
        return Stream.of(
            // {name, alias}
            arguments("Tsdb", "TsdbAlias"),      // name and alias matched
            arguments("Tsdb", "DifferentAlias"),  // existing name matched with given name
            arguments("DifferentName", "Tsdb"),   // existing name matched with given alias
            arguments("TsdbAlias", "DifferentAlias"), // existing alias matched with given name
            arguments("DifferentName", "TsdbAlias") // existing alias matched with given alias
        );
    }

    @Test
    public void editNamespace() throws IOException {
        long now = System.currentTimeMillis();
        Namespace namespace = getNamespace();
        final String originalName = namespace.getName();

        // create namespace and member
        final Integer namespaceId = createNamespaceWithMember(namespace, regularMember);

        // change values in namespace
        modifyNamespace(namespace);

        String url = endPoint + "/" + namespaceId;
        Response response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON).header(cookieHeader).body(namespace).put(url);

        assertEquals(200, response.getStatusCode(), response.getBody().asString());
        Namespace updatedNamespace = response.as(Namespace.class);

        // confirm json response
        // original name should not be modified
        assertNamespaceResponse(now, namespace, updatedNamespace, originalName, regularMember, regularMember);

        // confirm with db entry
        Namespace actualNamespace = dbUtil.getNamespaceById(namespaceId);
        assertStoredNamespace(now, updatedNamespace, actualNamespace);
    }

    @Test
    public void shouldThrowNotFoundExceptionOnModifyingNonExistentNamespace() {
        Namespace namespace = getNamespace();
        String url = endPoint + "/0";
        Response response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON).header(cookieHeader).body(namespace).put(url);
        assertEquals(NOT_FOUND.getStatusCode(), response.getStatusCode());
        assertEquals("Namespace not found", response.getBody().asString());
    }

    @Test
    public void rejectUnauthorizedEdit() throws IOException {
        Namespace namespace = getNamespace();
        // create namespace and member
        final Integer namespaceId = createNamespaceWithMember(namespace, regularMember);

        // change values in namespace
        modifyNamespace(namespace);

        String url = endPoint + "/" + namespaceId;
        Response response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON).header(unauthorizedCookieHeader).body(namespace).put(url);

        assertEquals(FORBIDDEN.getStatusCode(), response.getStatusCode()); // forbidden
        assertEquals("Namespace admin only allowed to modify namespace.", response.getBody().print());
    }

    /***********************************************************************************************************************************************************
                                                            NAMESPACE MEMBER INTEGRATION TEST
     **************************************************************************************************************************************************************/

    @Test
    public void getNamespaceMember() throws IOException {
        Namespace namespace = getNamespace();
        // create namespace and member
        final Integer namespaceId = createNamespaceWithMember(namespace, regularMember);

        String url = endPoint + "/" + namespaceId + "/member";
        Response response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON).header(cookieHeader).get(url).andReturn();

        assertEquals(200, response.getStatusCode());
        final User[] members = response.getBody().as(User[].class);
        assertEquals(1, members.length);
        assertEquals(regularMember, members[0].getUserid());
    }

    @Test
    public void addNamespaceMember() throws IOException {
        Namespace namespace = getNamespace();
        final Integer namespaceId = createNamespaceWithMember(namespace, regularMember);

        String url = endPoint + "/" + namespaceId + "/member";
        String payload = "[\"" +unauthorizedMember+ "\"]";
        Response response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON).header(cookieHeader).body(payload).post(url);
        assertEquals(response.getStatusCode(), 201);

        // ensure member present in database
        final List<String> namespaceMember = dbUtil.getNamespaceMember(namespaceId);
        assertEquals(namespaceMember.size(), 2);
        assertTrue(namespaceMember.get(0).equals("user.tsdb_test") || namespaceMember.get(1).equals("user.tsdb_test"));
    }

    @Test
    public void newMembersAbleToModifyNamespace() throws IOException {
        long now = System.currentTimeMillis();
        Namespace namespace = getNamespace();
        final String originalName = namespace.getName();
        final Integer namespaceId = createNamespaceWithMember(namespace, regularMember);

        String url = endPoint + "/" + namespaceId + "/member";
        String payload = "[\"" +unauthorizedMember+ "\"]";
        Response response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON).header(cookieHeader).body(payload).post(url);
        assertEquals(201, response.getStatusCode());

        // ensure member present in database
        final List<String> namespaceMember = dbUtil.getNamespaceMember(namespaceId);
        assertEquals(2, namespaceMember.size());
        assertTrue(namespaceMember.get(0).equals(unauthorizedMember) || namespaceMember.get(1).equals(unauthorizedMember));

        // ****************   test modifying namespace
        modifyNamespace(namespace);

        url = endPoint + "/" + namespaceId;
        response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON).header(unauthorizedCookieHeader).body(namespace).put(url);

        assertEquals(200, response.getStatusCode(), response.getBody().asString());
        Namespace updatedNamespace = response.as(Namespace.class);

        // confirm json response
        // original name should not be modified
        assertNamespaceResponse(now, namespace, updatedNamespace, originalName, regularMember, unauthorizedMember);

        // confirm with db entry
        Namespace actualNamespace = dbUtil.getNamespaceById(namespaceId);
        assertStoredNamespace(now, updatedNamespace, actualNamespace);
    }

    @Test
    public void addNamespaceCreatorAsMember() {
        Namespace namespace = getNamespace();
        Response response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON).header(cookieHeader).body(namespace).post(endPoint);
        assertEquals(response.getStatusCode(), 201, response.getBody().asString());
        Namespace createdNamespace = response.as(Namespace.class);

        // ensure member present in database
        final List<String> namespaceMember = dbUtil.getNamespaceMember(createdNamespace.getId());
        assertEquals(1, namespaceMember.size());
        assertEquals(regularMember, namespaceMember.get(0));
    }

    @Test
    public void removeNamespaceMember() throws IOException {
        Namespace namespace = getNamespace();
        final Integer namespaceId = createNamespaceWithMember(namespace, regularMember);
        dbUtil.insertNamespaceMember(namespaceId, "user.tsdb_test");

        String url = endPoint + "/" + namespaceId + "/member";
        String payload = "[\"" +unauthorizedMember+ "\"]";
        Response response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON).header(cookieHeader).body(payload).delete(url);
        assertEquals(204, response.getStatusCode());

        // ensure original member removed from database
        final List<String> namespaceMember = dbUtil.getNamespaceMember(namespaceId);
        assertEquals(1, namespaceMember.size());
        assertEquals(regularMember, namespaceMember.get(0));
    }

    @Test
    public void removeNewlyAddedMemberFromFollower() throws IOException {
        Namespace namespace = getNamespace();
        final Integer namespaceId = createNamespaceWithMember(namespace, regularMember);
        dbUtil.insertNamespaceFollower(namespaceId, "user.tsdb_test");

        String url = endPoint + "/" + namespaceId + "/member";
        String payload = "[\"" +unauthorizedMember+ "\"]";
        Response response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON).header(cookieHeader).body(payload).post(url);
        assertEquals(201, response.getStatusCode());

        // ensure member present in database
        final List<String> namespaceMember = dbUtil.getNamespaceMember(namespaceId);
        assertEquals(2, namespaceMember.size());
        assertTrue(namespaceMember.get(0).equals(unauthorizedMember) || namespaceMember.get(1).equals(unauthorizedMember));
        assertTrue(namespaceMember.get(0).equals(regularMember) || namespaceMember.get(1).equals(regularMember));

        // ensure add member removed from follower
        final List<String> namespaceFollower = dbUtil.getNamespaceFollower(namespaceId);
        assertEquals(0, namespaceFollower.size());
    }

    @Test
    public void rejectUnauthorizedUserAddingMembers() throws IOException {
        Namespace namespace = getNamespace();
        final Integer namespaceId = createNamespaceWithMember(namespace, regularMember);

        String url = endPoint + "/" + namespaceId + "/member";
        String payload = "[\"" +unauthorizedMember+ "\"]";
        Response response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON).header(unauthorizedCookieHeader).body(payload).post(url);
        assertEquals(403, response.getStatusCode());
        assertEquals(response.getBody().print(), "Namespace admin only allowed to add members.");
    }

    @Test
    public void rejectUnauthorizedUserRemovingMembers() throws IOException {
        Namespace namespace = getNamespace();
        final Integer namespaceId = createNamespaceWithMember(namespace, regularMember);

        String url = endPoint + "/" + namespaceId + "/member";
        String payload = "[\"" +regularMember+ "\"]";
        Response response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON).header(unauthorizedCookieHeader).body(payload).delete(url);
        assertEquals(403, response.getStatusCode());
        assertEquals(response.getBody().print(), "Namespace admin only allowed to remove member.");
    }

    /***********************************************************************************************************************************************************
                                                            NAMESPACE FOLLOWER INTEGRATION TEST
     **************************************************************************************************************************************************************/

    @Test
    public void getNamespaceFollower() throws IOException {
        Namespace namespace = getNamespace();
        // create namespace and member
        final Integer namespaceId = createNamespaceWithMember(namespace, regularMember);
        dbUtil.insertNamespaceFollower(namespaceId, unauthorizedMember);

        String url = endPoint + "/" + namespaceId + "/follower";
        Response response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON).header(unauthorizedCookieHeader).get(url).andReturn();

        assertEquals(200, response.getStatusCode());
        final User[] follower = response.getBody().as(User[].class);
        assertEquals(follower.length, 1);
        assertEquals(follower[0].getUserid(), unauthorizedMember);
    }

    @Test
    public void addNamespaceFollower() throws IOException {
        Namespace namespace = getNamespace();
        final Integer namespaceId = createNamespaceWithMember(namespace, regularMember);

        String url = endPoint + "/" + namespaceId + "/follower";
        Response response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON).header(unauthorizedCookieHeader).post(url).andReturn();
        assertEquals(CREATED.getStatusCode(), response.getStatusCode());

        // ensure follower present in database
        final List<String> namespaceFollower = dbUtil.getNamespaceFollower(namespaceId);
        assertEquals(namespaceFollower.size(), 1);
    }

    @Test
    public void removeNamespaceFollower() throws IOException {
        Namespace namespace = getNamespace();
        // create namespace and member
        final Integer namespaceId = createNamespaceWithMember(namespace, regularMember);
        dbUtil.insertNamespaceFollower(namespaceId, unauthorizedMember);

        String url = endPoint + "/" + namespaceId + "/follower";
        Response response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON).header(unauthorizedCookieHeader).delete(url).andReturn();

        // follower should not present in database
        assertEquals(204, response.getStatusCode());
        final List<String> namespaceFollower = dbUtil.getNamespaceFollower(namespaceId);
        assertEquals(0, namespaceFollower.size());
    }

    @Test
    public void shouldNotAddMemberAsFollower() throws IOException {
        Namespace namespace = getNamespace();
        // create namespace and member
        final Integer namespaceId = createNamespaceWithMember(namespace, regularMember);

        String url = endPoint + "/" + namespaceId + "/follower";
        Response response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON).header(cookieHeader).post(url).andReturn();

        assertEquals(400, response.getStatusCode());

        // follower should not present in database
        final List<String> namespaceFollower = dbUtil.getNamespaceFollower(namespaceId);
        assertEquals(0, namespaceFollower.size());
    }

    private void assertNamespaceResponse(long now, Namespace namespace, Namespace createdNamespace, String namespaceName, String createdBy, String updatedBy) {
        assertEquals(createdNamespace.getName(), namespaceName);
        assertEquals(createdNamespace.getAlias(), slugify(namespace.getAlias()));
        assertEquals(createdNamespace.getMeta(), namespace.getMeta());
        assertEquals(createdNamespace.getEnabled(), namespace.getEnabled());
        assertEquals(createdNamespace.getCreatedBy(), createdBy);
        assertEquals(createdNamespace.getUpdatedBy(), updatedBy);
        assertTrue(createdNamespace.getCreatedTime().getTime() >= now);
        assertTrue(createdNamespace.getUpdatedTime().getTime() >= now);
    }

    private void assertStoredNamespace(long now, Namespace updatedNamespace, Namespace actualNamespace) {
        assertEquals(actualNamespace.getId(), updatedNamespace.getId());
        assertEquals(actualNamespace.getName(), updatedNamespace.getName());
        assertEquals(actualNamespace.getAlias(), updatedNamespace.getAlias());
        assertEquals(actualNamespace.getMeta(), updatedNamespace.getMeta());
        assertEquals(actualNamespace.getEnabled(), updatedNamespace.getEnabled());
        assertEquals(actualNamespace.getCreatedBy(), updatedNamespace.getCreatedBy());
        assertEquals(actualNamespace.getUpdatedBy(), updatedNamespace.getUpdatedBy());
        assertTrue(actualNamespace.getCreatedTime().getTime() >= now);
        assertTrue(actualNamespace.getUpdatedTime().getTime() >= now);
    }

    private void modifyNamespace(Namespace namespace) {
        // change values in namespace
        namespace.setName("NewNamespace");
        namespace.setAlias("new-alias");
        namespace.getMeta().put(NamespaceService.DH_TRACKS, Arrays.asList("new-tracks"));
        namespace.setEnabled(false);
       // namespace.setMeta("new-meta");
    }
}
