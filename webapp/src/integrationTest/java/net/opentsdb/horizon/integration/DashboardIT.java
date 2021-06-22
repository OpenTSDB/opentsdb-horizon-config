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
import net.opentsdb.horizon.fs.model.Content;
import net.opentsdb.horizon.fs.model.File;
import net.opentsdb.horizon.fs.model.FileHistory;
import net.opentsdb.horizon.fs.model.Folder;
import net.opentsdb.horizon.fs.view.FileDto;
import net.opentsdb.horizon.fs.view.FolderDto;
import net.opentsdb.horizon.fs.view.FolderType;
import net.opentsdb.horizon.model.Favorite;
import net.opentsdb.horizon.model.FolderActivity;
import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.view.UserFolderDto;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.jayway.restassured.RestAssured.given;
import static net.opentsdb.horizon.util.Utils.deSerialize;
import static net.opentsdb.horizon.util.Utils.decompress;
import static net.opentsdb.horizon.util.Utils.slugify;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DashboardIT extends BaseIT {

  private Folder userHomeFolder;
  private Folder userTrashFolder;

  private Folder otherUserHomeFolder;
  private Folder otherUserTrashFolder;

  private Namespace namespace;
  private Folder namespaceHomeFolder;
  private Folder namespaceTrashFolder;

  private Namespace otherNamespace;
  private Folder otherNamesapceHomeFolder;
  private Folder otherNamesapceTrashFolder;

  private Timestamp timestamp;

  @Override
  protected String getUri() {
    return "dashboard";
  }

  @BeforeAll
  public void beforeAll() throws IOException {
    timestamp = new Timestamp(System.currentTimeMillis());

    userHomeFolder = createUserHomeFolder(regularMember, timestamp);
    userTrashFolder = createTrashFolder(userHomeFolder);
    long homeId = dbUtil.insert(userHomeFolder);
    long trashId = dbUtil.insert(userTrashFolder);
    userHomeFolder.setId(homeId);
    userTrashFolder.setId(trashId);

    otherUserHomeFolder = createUserHomeFolder(unauthorizedMember, timestamp);
    otherUserTrashFolder = createTrashFolder(otherUserHomeFolder);
    homeId = dbUtil.insert(otherUserHomeFolder);
    trashId = dbUtil.insert(otherUserTrashFolder);
    otherUserHomeFolder.setId(homeId);
    otherUserTrashFolder.setId(trashId);

    namespace = createNamespace("namesapce 1", "test track", regularMember, timestamp);
    namespaceHomeFolder = createNamespaceHomeFolder(namespace.getAlias(), regularMember, timestamp);
    namespaceTrashFolder = createTrashFolder(namespaceHomeFolder);
    homeId = dbUtil.insert(namespaceHomeFolder);
    trashId = dbUtil.insert(namespaceTrashFolder);
    namespaceHomeFolder.setId(homeId);
    namespaceTrashFolder.setId(trashId);

    int namespaceId = dbUtil.insert(namespace);
    namespace.setId(namespaceId);
    dbUtil.insertNamespaceMember(namespaceId, regularMember);

    otherNamespace = createNamespace("namesapce 2", "test track", unauthorizedMember, timestamp);
    otherNamesapceHomeFolder =
        createNamespaceHomeFolder(otherNamespace.getAlias(), unauthorizedMember, timestamp);
    otherNamesapceTrashFolder = createTrashFolder(otherNamesapceHomeFolder);
    homeId = dbUtil.insert(otherNamesapceHomeFolder);
    trashId = dbUtil.insert(otherNamesapceTrashFolder);
    otherNamesapceHomeFolder.setId(homeId);
    otherNamesapceTrashFolder.setId(trashId);

    namespaceId = dbUtil.insert(otherNamespace);
    otherNamespace.setId(namespaceId);
    dbUtil.insertNamespaceMember(namespaceId, unauthorizedMember);
  }

  @BeforeEach
  public void beforeMethod() {
    dbUtil.clearTable("folder_activity");
    dbUtil.clearTable("favorite_folder");
    dbUtil.clearTable("folder_history");
    dbUtil.execute("DELETE FROM folder WHERE name != 'Home' AND name != 'Trash'");
    dbUtil.clearTable("content");
  }

  @Test
  void createPersonalFolder() {
    long now = System.currentTimeMillis();
    FolderDto folderDto = new FolderDto();
    folderDto.setName("Folder 1");

    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(folderDto)
            .put(endPoint + "/folder");
    assertEquals(201, response.getStatusCode());
    FolderDto fromServer = response.as(FolderDto.class);
    assertFolder(now, folderDto.getName(), userHomeFolder.getPath(), fromServer);

    Folder fromDB = dbUtil.getFolderById(fromServer.getId());
    assertFolderEquals(fromServer, fromDB);
    assertNull(fromDB.getContentid());
  }

  @Test
  void createPersonalSubFolder() {
    long now = System.currentTimeMillis();
    Folder parentFolder = createFolder("Folder 1", new Timestamp(now), userHomeFolder);
    long parentId = dbUtil.insert(parentFolder);
    parentFolder.setId(parentId);

    FolderDto folderDto = new FolderDto();
    folderDto.setName("Folder 1 1");
    folderDto.setParentId(parentId);

    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(folderDto)
            .put(endPoint + "/folder");
    assertEquals(201, response.getStatusCode());
    FolderDto fromServer = response.as(FolderDto.class);
    assertFolder(now, folderDto.getName(), parentFolder.getPath(), fromServer);

    Folder fromDB = dbUtil.getFolderById(fromServer.getId());
    assertFolderEquals(fromServer, fromDB);
    assertNull(fromDB.getContentid());
  }

  @Test
  void createPersonalDashboard() throws IOException {
    long now = System.currentTimeMillis();
    Map<String, String> content = createContentMap();

    FileDto fileDto = new FileDto();
    fileDto.setName("Dashboard 1");
    fileDto.setContent(content);
    fileDto.setParentId(userHomeFolder.getId());

    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(fileDto)
            .put(endPoint + "/file");
    assertEquals(201, response.getStatusCode());
    FileDto fromServer = response.as(FileDto.class);
    assertDashboard(now, fileDto.getName(), userHomeFolder.getPath(), content, fromServer);

    File fromDB = (File) dbUtil.getFolderById(fromServer.getId());
    assertFileEquals(fromServer, fromDB);
  }

  @ParameterizedTest(name = "[{index}] add {2} to favorites")
  @MethodSource("buildDashboardsAndFolders")
  void addDashboardAndFoldersToFavorites(
      final Folder dashboard, final Content content, final String displayName) {
    if (null != content) {
      dbUtil.insert(content);
    }
    long dashboardId = dbUtil.insert(dashboard);
    dashboard.setId(dashboardId);

    FileDto fileDto = new FileDto();
    fileDto.setId(dashboardId);

    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(fileDto)
            .post(endPoint + "/favorite");
    assertEquals(201, response.getStatusCode());

    Favorite favorite = dbUtil.getFavoriteFolder(dashboardId).get();
    assertTrue(favorite.getId() >= 0);
    assertEquals(regularMember, favorite.getUserId());
    assertEquals(dashboardId, favorite.getFolderId());
    assertTrue(favorite.getCreatedTime().getTime() >= dashboard.getCreatedTime().getTime());
  }

  @Test
  void removeDashboardFromFavorites() throws IOException {
    Timestamp now = new Timestamp(System.currentTimeMillis());
    Map<String, String> map = createContentMap();

    Content content = createContent(map, now, userHomeFolder.getCreatedBy());
    File dashboard = createDashboard("Dashboard 1", content.getSha2(), now, userHomeFolder);
    dbUtil.insert(content);
    long dashboardId = dbUtil.insert(dashboard);
    dashboard.setId(dashboardId);
    FileHistory fileHistory = createFileHistory(dashboard);
    dbUtil.insert(fileHistory);
    Favorite favorite = createFavoriteFolder(regularMember, dashboardId, now);
    dbUtil.insert(favorite);

    FileDto fileDto = new FileDto();
    fileDto.setId(dashboardId);

    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .body(fileDto)
            .delete(endPoint + "/favorite");
    assertEquals(204, response.getStatusCode());

    Optional<Favorite> fromDB = dbUtil.getFavoriteFolder(dashboardId);
    assertFalse(fromDB.isPresent());
  }

  @ParameterizedTest(name = "[{index}] get favorite {2} by id")
  @MethodSource("buildDashboardsAndFolders")
  void getFavoriteDashboardsAndFolderById(
      final Folder dashboard, final Content content, final String displayName) {

    Timestamp now = new Timestamp(System.currentTimeMillis());
    if (null != content) {
      dbUtil.insert(content);
    }
    long dashboardId = dbUtil.insert(dashboard);
    dashboard.setId(dashboardId);

    Favorite favorite = createFavoriteFolder(regularMember, dashboardId, now);
    dbUtil.insert(favorite);

    String url = endPoint + (null == content ? "/folder/" : "/file/") + dashboardId;
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(url);
    assertEquals(200, response.getStatusCode());
    FolderDto fromServer = response.as(FolderDto.class);
    assertTrue(fromServer.getFavorite());
  }

  @Test
  void getMyFavoriteDashboardsAndFolders() throws IOException {
    Timestamp now = new Timestamp(System.currentTimeMillis());
    Stream<Arguments> arguments = buildDashboardsAndFolders();
    Map<String, Arguments> argMap =
        arguments.collect(
            Collectors.toMap(arg -> ((Folder) arg.get()[0]).getName(), Function.identity()));

    List<String> favoriteNames =
        Arrays.asList(
            "Namespace Dashboard 1", "Folder 1", "Dashboard 2"); // add these 3 to the favorites

    argMap.values().stream()
        .forEach(
            argument -> {
              Folder folder = (Folder) argument.get()[0];
              Content content = (Content) argument.get()[1];
              if (null != content) {
                dbUtil.insert(content);
                File file = (File) argument.get()[0];
                file.setContentid(content.getSha2());
                folder = file;
              }

              long dashboardId = dbUtil.insert(folder);
              folder.setId(dashboardId);

              String name = folder.getName();
              if (favoriteNames.contains(name)) {
                Favorite favorite = createFavoriteFolder(regularMember, dashboardId, now);
                dbUtil.insert(favorite);
              }
            });

    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(endPoint + "/favorite");
    assertEquals(200, response.getStatusCode());

    UserFolderDto fromServer = response.as(UserFolderDto.class);
    assertEquals(regularMember, fromServer.getUser().getUserid());
    List<FolderDto> actualList = fromServer.getFavorites();
    assertEquals(3, actualList.size()); // only 3 were added to favorites

    Map<String, FolderDto> favoriteMap =
        actualList.stream().collect(Collectors.toMap(f -> f.getName(), Function.identity()));

    // verify that you got only the favorites
    for (String favoriteName : favoriteNames) {
      FolderDto actual = favoriteMap.get(favoriteName);
      assertFolderEquals((Folder) argMap.get(actual.getName()).get()[0], actual);
      assertEquals(now, actual.getFavoritedTime());
    }
  }

  @Test
  void recordLastVisitedTimeUponReadingADashboard() throws IOException, InterruptedException {

    Map<String, String> map = createContentMap();
    Content content = createContent(map, timestamp, userHomeFolder.getCreatedBy());

    File dashboard = createDashboard("Dashboard 1", content.getSha2(), timestamp, userHomeFolder);
    dbUtil.insert(content);
    dashboard.setContentid(content.getSha2());

    long dashboardId = dbUtil.insert(dashboard);
    dashboard.setId(dashboardId);

    Timestamp ts1 = new Timestamp(System.currentTimeMillis());
    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(endPoint + "/file/" + dashboardId);
    assumeTrue(200 == response.getStatusCode());

    Thread.sleep(3000); // wait for the dashboard activity updated in async.

    FolderActivity folderActivity = dbUtil.getFolderActivity(regularMember, dashboardId);
    assertEquals(regularMember, folderActivity.getUserId());
    assertEquals(dashboardId, folderActivity.getFolderId());
    Timestamp lastVisitedTime = folderActivity.getLastVisitedTime();
    assertTrue(ts1.getTime() <= lastVisitedTime.getTime());

    // verify that reading again, updates the last visited time of the same record.
    response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(endPoint + "/file/" + dashboardId);
    assumeTrue(200 == response.getStatusCode());

    folderActivity = dbUtil.getFolderActivity(regularMember, dashboardId);
    assertEquals(regularMember, folderActivity.getUserId());
    assertEquals(dashboardId, folderActivity.getFolderId());
    assertTrue(lastVisitedTime.getTime() <= folderActivity.getLastVisitedTime().getTime());
  }

  @Test
  void getMyRecentlyVisitedDashboardsOrderedByLastVisitedTime()
      throws IOException, InterruptedException {
    Stream<Arguments> arguments = buildDashboards();
    Map<String, Arguments> argMap =
        arguments.collect(
            Collectors.toMap(arg -> ((Folder) arg.get()[0]).getName(), Function.identity()));
    argMap.values().stream()
        .forEach(
            argument -> {
              Content content = (Content) argument.get()[1];
              File file = (File) argument.get()[0];
              file.setContentid(content.getSha2());
              dbUtil.insert(content);
              long dashboardId = dbUtil.insert(file);
              file.setId(dashboardId);
            });

    List<String> recentDashboardNames =
        Arrays.asList("Dashboard 1", "Namespace Dashboard 2"); // add these 2 to the activities

    for (int i = 0; i < recentDashboardNames.size(); i++) {
      File dashboard = (File) argMap.get(recentDashboardNames.get(i)).get()[0];
      Timestamp lastVisitedTime = new Timestamp(timestamp.getTime() - i * 10);
      dbUtil.insert(createFolderActivity(regularMember, dashboard.getId(), lastVisitedTime));
    }

    File dashboard = (File) argMap.get("Dashboard 2").get()[0];
    Timestamp lastVisitedTime = new Timestamp(timestamp.getTime() + 1);
    dbUtil.insert(
        createFolderActivity(
            unauthorizedMember,
            dashboard.getId(),
            lastVisitedTime)); // activity for a different user

    Thread.sleep(3000); // wait for the dashboard activity updated in async.

    Response response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(endPoint + "/recent");
    assumeTrue(200 == response.getStatusCode());
    UserFolderDto fromServer = response.getBody().as(UserFolderDto.class);

    assertEquals(regularMember, fromServer.getUser().getUserid());
    List<FolderDto> recentDashboards = fromServer.getRecent();
    assertEquals(2, recentDashboards.size());
    for (int i = 0; i < recentDashboards.size(); i++) {
      FolderDto recent = recentDashboards.get(i);
      assertFolderEquals((Folder) argMap.get(recent.getName()).get()[0], recent);
      assertEquals(new Timestamp(timestamp.getTime() - i * 10), recent.getLastVisitedTime());
    }

    // get recently visited dashboards for a different user
    response =
        given()
            .config(ignoreSslCertificateValidation)
            .contentType(ContentType.JSON)
            .header(cookieHeader)
            .get(endPoint + "/recent?userId=" + unauthorizedMember);
    assumeTrue(200 == response.getStatusCode());
    fromServer = response.getBody().as(UserFolderDto.class);
    assertEquals(unauthorizedMember, fromServer.getUser().getUserid());
    recentDashboards = fromServer.getRecent();
    assertEquals(1, recentDashboards.size());
    FolderDto recent = recentDashboards.get(0);
    assertFolderEquals((Folder) argMap.get(recent.getName()).get()[0], recent);
    assertEquals(new Timestamp(timestamp.getTime() + 1), recent.getLastVisitedTime());
  }

  private FolderActivity createFolderActivity(
      String userId, long folderId, Timestamp lastVisitedTime) {
    FolderActivity folderActivity = new FolderActivity();
    folderActivity.setUserId(userId);
    folderActivity.setFolderId(folderId);
    folderActivity.setLastVisitedTime(lastVisitedTime);
    return folderActivity;
  }

  private Stream<Arguments> buildDashboardsAndFolders() throws IOException {
    Stream<Arguments> files = buildDashboards();
    Stream<Arguments> folders = buildFolders();
    return Stream.concat(files, folders);
  }

  private Stream<Arguments> buildDashboards() throws IOException {
    Timestamp now = timestamp;
    Map<String, String> map = createContentMap();
    Content content = createContent(map, now, userHomeFolder.getCreatedBy());

    File personalDashboard = createDashboard("Dashboard 1", content.getSha2(), now, userHomeFolder);
    File namespaceDashboard =
        createDashboard("Namespace Dashboard 1", content.getSha2(), now, namespaceHomeFolder);
    File otherUserDashboard =
        createDashboard("Dashboard 2", content.getSha2(), now, otherUserHomeFolder);
    File otherNamespaceDashboard =
        createDashboard("Namespace Dashboard 2", content.getSha2(), now, otherNamesapceHomeFolder);

    return Stream.of(
        arguments(personalDashboard, content, "personal dashboard"),
        arguments(namespaceDashboard, content, "namespace dashboard"),
        arguments(otherUserDashboard, content, "other's personal dashboard"),
        arguments(otherNamespaceDashboard, content, "other's namespace dashboard"));
  }

  private Stream<Arguments> buildFolders() {
    Timestamp now = timestamp;
    Folder personalFolder = createFolder("Folder 1", now, userHomeFolder);
    Folder namespaceFolder = createFolder("Namespace Folder 1", now, namespaceHomeFolder);
    Folder othersPersonalFolder = createFolder("Folder 2", now, otherUserHomeFolder);
    Folder othersNamespaceFolder =
        createFolder("Namespace Folder 2", now, otherNamesapceHomeFolder);

    return Stream.of(
        arguments(personalFolder, null, "personal folder"),
        arguments(namespaceFolder, null, "namespace folder"),
        arguments(othersPersonalFolder, null, "other's personal folder"),
        arguments(othersNamespaceFolder, null, "other's namespace folder"));
  }

  private Map<String, String> createContentMap() {
    Map<String, String> map = new HashMap<>();
    map.put("k1", "v1");
    map.put("k2", "v2");
    return map;
  }

  private void assertDashboard(
      long now, String name, String parentPath, Object content, FileDto actual) {
    assertFolder(now, name, parentPath, actual);
    assertEquals(content, actual.getContent());
  }

  private void assertFolder(long now, String name, String parentPath, FolderDto actual) {
    String slug = slugify(name);
    assertTrue(actual.getId() > 0);
    assertEquals(name, actual.getName());
    assertEquals(FolderType.DASHBOARD, actual.getType());
    assertEquals("/" + actual.getId() + "/" + slug, actual.getPath());
    assertEquals(parentPath + "/" + slug, actual.getFullPath());
    assertEquals(regularMember, actual.getCreatedBy());
    assertEquals(regularMember, actual.getUpdatedBy());
    Timestamp createdTime = actual.getCreatedTime();
    assertTrue(createdTime.getTime() >= now);
    assertEquals(createdTime, actual.getUpdatedTime());
  }

  private void assertFileEquals(FileDto expected, File actual) throws IOException {
    assertFolderEquals(expected, actual);
    Content content = dbUtil.getContentById(actual.getContentid());
    assertEquals(expected.getCreatedBy(), content.getCreatedby());
    assertEquals(expected.getCreatedTime(), content.getCreatedtime());
    byte[] decompressed = decompress(content.getData());
    Object deSerialized = deSerialize(decompressed, Object.class);
    assertEquals(expected.getContent(), deSerialized);
    FileHistory fileHistory = dbUtil.getHistoryByFileId(expected.getId());
    assertTrue(fileHistory.getId() > 0);
    assertEquals(expected.getId(), fileHistory.getFileid());
    assertArrayEquals(actual.getContentid(), fileHistory.getContentid());
    // round to seconds for easier comparision as the user table stores it in seconds.
    long seconds = roundToSecond(actual.getCreatedTime());
    assertEquals(new Timestamp(seconds), fileHistory.getCreatedtime());
  }

  private void assertFolderEquals(FolderDto expected, Folder actual) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getName(), actual.getName());
    assertEquals(expected.getType(), actual.getType());
    String path = expected.getFullPath();
    String parentPath = path.substring(0, path.lastIndexOf("/"));
    assertEquals(path, actual.getPath());
    assertArrayEquals(md5.digest(actual.getPath().getBytes()), actual.getPathHash());
    assertArrayEquals(md5.digest(parentPath.getBytes()), actual.getParentPathHash());
    assertEquals(expected.getCreatedBy(), actual.getCreatedBy());
    assertEquals(expected.getCreatedTime(), actual.getCreatedTime());
    assertEquals(expected.getUpdatedBy(), actual.getUpdatedBy());
    assertEquals(expected.getUpdatedTime(), actual.getUpdatedTime());
  }

  private void assertFolderEquals(Folder expected, FolderDto actual) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getName(), actual.getName());
    assertEquals(expected.getType(), actual.getType());
    String fullPath = actual.getFullPath();
    String parentPath = fullPath.substring(0, fullPath.lastIndexOf("/"));
    assertEquals(expected.getPath(), fullPath);
    assertArrayEquals(expected.getPathHash(), md5.digest(actual.getFullPath().getBytes()));
    assertArrayEquals(expected.getParentPathHash(), md5.digest(parentPath.getBytes()));
    assertEquals(expected.getCreatedBy(), actual.getCreatedBy());
    assertEquals(expected.getCreatedTime(), actual.getCreatedTime());
    assertEquals(expected.getUpdatedBy(), actual.getUpdatedBy());
    assertEquals(expected.getUpdatedTime(), actual.getUpdatedTime());
  }
}
