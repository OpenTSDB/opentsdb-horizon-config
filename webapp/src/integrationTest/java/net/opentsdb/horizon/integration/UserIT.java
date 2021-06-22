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
import com.jayway.restassured.response.Header;
import com.jayway.restassured.response.Response;
import net.opentsdb.horizon.fs.model.Folder;
import net.opentsdb.horizon.model.User;
import net.opentsdb.horizon.model.User.CreationMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.Arrays;

import static com.jayway.restassured.RestAssured.given;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UserIT extends BaseIT {

  @Override
  protected String getUri() {
    return "user";
  }

  @BeforeEach
  public void beforeMethod() {
    cleanUpDB();
  }

  @Test
  public void createUser() {
    long now = System.currentTimeMillis();

    String userId = "user.foo";
    User user =
        new User()
            .setUserid(userId)
            .setName("Foo Bar")
            .setCreationmode(CreationMode.onthefly)
            .setEnabled(true);

    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(Arrays.asList(user))
            .post(endPoint + "/list");

    assertEquals(201, response.getStatusCode(), response.getBody().asString());
    User[] users = response.as(User[].class);
    assertEquals(1, users.length);

    User userFromServer = users[0];
    Timestamp userCreationTime = userFromServer.getUpdatedtime();

    assertEquals(userFromServer.getUserid(), user.getUserid());
    assertEquals(userFromServer.getName(), user.getName());
    assertEquals(userFromServer.getCreationmode(), user.getCreationmode());
    assertTrue(userFromServer.isEnabled());
    assertTrue(now <= userCreationTime.getTime());
    assertNull(userFromServer.getDisabledtime());

    // round to seconds for easier comparision as the user table stores it in seconds.
    long seconds = roundToSecond(userCreationTime);
    userFromServer.setUpdatedtime(new Timestamp(seconds));

    User userFromDB = dbUtil.getUserById(userFromServer.getUserid());
    assertUserEquals(userFromServer, userFromDB);

    Folder expectedUserHomeFolder = createUserHomeFolder(userId, userCreationTime);
    Folder expectedUserTrashFolder = createTrashFolder(expectedUserHomeFolder);

    Folder homeFolder = dbUtil.getFolderByName(expectedUserHomeFolder.getName());
    Folder trashFolder = dbUtil.getFolderByName(expectedUserTrashFolder.getName());

    expectedUserHomeFolder.setId(homeFolder.getId());
    expectedUserTrashFolder.setId(trashFolder.getId());

    assertFolderEquals(expectedUserHomeFolder, homeFolder);
    assertFolderEquals(expectedUserTrashFolder, trashFolder);
  }

  @Test
  public void userIdIsUnique() {
    String userId = "user.foo";
    User user =
        new User().setUserid(userId).setName("Foo Bar").setCreationmode(CreationMode.onthefly);
    user.setEnabled(true);
    user.setUpdatedtime(new Timestamp(System.currentTimeMillis()));
    dbUtil.insert(user);

    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(Arrays.asList(user))
            .post(endPoint + "/list");

    String message = response.getBody().asString();
    assertEquals(CONFLICT.getStatusCode(), response.getStatusCode(), message);
    assertEquals("Duplicate user id: " + userId, message);
  }

  @Test
  void provisionNewUser() {
    long now = System.currentTimeMillis();

    String userId = "user.foo";
    Header cookie = new Header("Cookie", createOktaCookie(userId));

    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookie)
            .put(endPoint)
            .andReturn();
    assertEquals(200, response.getStatusCode());
    User userFromServer = response.as(User.class);
    Timestamp userCreationTime = userFromServer.getUpdatedtime();

    assertEquals(userId, userFromServer.getUserid());
    assertEquals("", userFromServer.getName());
    assertEquals(CreationMode.onthefly, userFromServer.getCreationmode());
    assertTrue(userFromServer.isEnabled());
    assertTrue(now <= userCreationTime.getTime());
    assertNull(userFromServer.getDisabledtime());

    // round to seconds for easier comparision as the user table stores it in seconds.
    long seconds = roundToSecond(userCreationTime);
    userFromServer.setUpdatedtime(new Timestamp(seconds));

    User userFromDB = dbUtil.getUserById(userFromServer.getUserid());
    assertUserEquals(userFromServer, userFromDB);

    Folder expectedUserHomeFolder = createUserHomeFolder(userId, userCreationTime);
    Folder expectedUserTrashFolder = createTrashFolder(expectedUserHomeFolder);

    Folder homeFolder = dbUtil.getFolderByName(expectedUserHomeFolder.getName());
    Folder trashFolder = dbUtil.getFolderByName(expectedUserTrashFolder.getName());

    expectedUserHomeFolder.setId(homeFolder.getId());
    expectedUserTrashFolder.setId(trashFolder.getId());

    assertFolderEquals(expectedUserHomeFolder, homeFolder);
    assertFolderEquals(expectedUserTrashFolder, trashFolder);

    // verify idempotent.
    response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookie)
            .put(endPoint)
            .andReturn();
    assertEquals(200, response.getStatusCode());
    User userFromServer2 = response.as(User.class);
    assertUserEquals(userFromServer, userFromServer2);

    Folder homeFolder2 = dbUtil.getFolderByName(expectedUserHomeFolder.getName());
    Folder trashFolder2 = dbUtil.getFolderByName(expectedUserTrashFolder.getName());
    assertFolderEquals(expectedUserHomeFolder, homeFolder2);
    assertFolderEquals(expectedUserTrashFolder, trashFolder2);
  }

  @Test
  public void getById() {
    User user =
        new User().setUserid("user.foo").setName("Foo Bar").setCreationmode(CreationMode.onthefly);
    user.setEnabled(true);
    user.setUpdatedtime(new Timestamp(System.currentTimeMillis()));
    dbUtil.insert(user);

    String url = endPoint + "/" + user.getUserid();
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(url)
            .andReturn();

    assertEquals(200, response.getStatusCode());
    User actualUser = response.as(User.class);

    assertEquals(actualUser.getUserid(), user.getUserid());
    assertEquals(actualUser.getName(), user.getName());
    assertEquals(actualUser.getCreationmode(), user.getCreationmode());
    assertTrue(actualUser.isEnabled());
    assertNotNull(actualUser.getUpdatedtime());
    assertNull(actualUser.getDisabledtime());
  }

  private void assertFolderEquals(Folder expected, Folder actual) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getName(), actual.getName());
    assertEquals(expected.getType(), actual.getType());
    assertEquals(expected.getPath(), actual.getPath());
    assertArrayEquals(expected.getPathHash(), actual.getPathHash());
    assertArrayEquals(expected.getParentPathHash(), actual.getParentPathHash());
    assertEquals(expected.getContentid(), actual.getContentid());
    assertEquals(expected.getCreatedBy(), actual.getCreatedBy());
    assertEquals(expected.getCreatedTime(), actual.getCreatedTime());
    assertEquals(expected.getUpdatedBy(), actual.getUpdatedBy());
    assertEquals(expected.getUpdatedTime(), actual.getUpdatedTime());
  }

  private void assertUserEquals(User expected, User actual) {
    assertEquals(expected.getUserid(), actual.getUserid());
    assertEquals(expected.getName(), actual.getName());
    assertEquals(expected.getCreationmode(), actual.getCreationmode());
    assertEquals(expected.isEnabled(), actual.isEnabled());
    assertEquals(expected.getUpdatedtime(), actual.getUpdatedtime());
    assertEquals(expected.getDisabledtime(), actual.getDisabledtime());
  }
}
