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
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.config.RestAssuredConfig;
import com.jayway.restassured.config.SSLConfig;
import com.jayway.restassured.response.Header;
import com.mysql.cj.jdbc.MysqlDataSource;
import com.oath.auth.KeyRefresher;
import com.oath.auth.Utils;
import net.opentsdb.horizon.config.Config;
import net.opentsdb.horizon.config.ConfigLoader;
import net.opentsdb.horizon.config.DBConfig;
import net.opentsdb.horizon.config.Env;
import net.opentsdb.horizon.config.ITConfig;
//import net.opentsdb.horizon.filter.DebugOktaFilter;
import net.opentsdb.horizon.fs.model.Content;
import net.opentsdb.horizon.fs.model.File;
import net.opentsdb.horizon.fs.model.FileHistory;
import net.opentsdb.horizon.fs.model.Folder;
import net.opentsdb.horizon.fs.view.FolderType;
import net.opentsdb.horizon.model.Alert;
import net.opentsdb.horizon.model.Contact;
import net.opentsdb.horizon.model.Favorite;
import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.model.User;
import net.opentsdb.horizon.service.NamespaceService;
import net.opentsdb.horizon.util.DBUtil;
import net.opentsdb.horizon.secrets.DebugKeyReader;
import net.opentsdb.horizon.secrets.KeyException;
import net.opentsdb.horizon.secrets.KeyReader;
import com.yahoo.athenz.zts.ZTSClient;
//import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.opentsdb.horizon.ApplicationFactory.formatJdbcProperties;
import static net.opentsdb.horizon.service.NamespaceService.DH_TRACKS;
import static net.opentsdb.horizon.util.Utils.compress;
import static net.opentsdb.horizon.util.Utils.serialize;
import static net.opentsdb.horizon.util.Utils.slugify;

@TestInstance(Lifecycle.PER_CLASS)
public abstract class BaseIT {

  protected static DBUtil dbUtil;
  protected static String baseUrl;
  protected static Header cookieHeader;
  protected static Header unauthorizedCookieHeader;
  protected static final String regularMember = "user.tsdb_integration";
  protected static final String unauthorizedMember = "user.tsdb_test";
  private static final String ztsUrl = "https://zts.opentsdb.net";
  private static final String zmsUrl = "https://zms.opentsdb.net/zms/v1";
  private static KeyReader keyReader;
  protected static RestAssuredConfig ignoreSslCertificateValidation;
  protected static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  protected String endPoint;
  protected MessageDigest md5;
  protected MessageDigest sha256;
  protected Timestamp timestamp;

  static {
    try {
      ITConfig itConfig = ConfigLoader.loadResourceConfig("config-it.yaml", ITConfig.class);
      Env env = Env.valueOf(System.getProperties().getProperty("env", Env.dev.name()));
      Config config = ConfigLoader.loadResourceConfig("config-" + env.name() + ".yaml", Config.class);

      SSLConfig sslConfig = new SSLConfig().allowAllHostnames();
      if (env == Env.dev) {
        sslConfig = sslConfig.relaxedHTTPSValidation();
      }
      ignoreSslCertificateValidation = RestAssured.config().sslConfig(sslConfig);

      String key = itConfig.key;
      String userHome = System.getProperty("user.home");
      if (key.startsWith("~")) {
        key = key.replaceFirst("~", userHome);
      }

      String cert = itConfig.cert;
      if (cert.startsWith("~")) {
        cert = cert.replaceFirst("~", userHome);
      }

      String cacert = itConfig.cacert;
      if (cacert.startsWith("~")) {
        cacert = cacert.replaceFirst("~", userHome);
      }

      if (env == Env.dev) {
        keyReader = new DebugKeyReader();
      } else {
        KeyRefresher keyRefresher = Utils.generateKeyRefresher(cacert, "changeit", cert, key);
        keyRefresher.startup();
        SSLContext sslContext =
            Utils.buildSSLContext(
                keyRefresher.getKeyManagerProxy(), keyRefresher.getTrustManagerProxy());
        ZTSClient ztsClient = new ZTSClient(ztsUrl, sslContext);

        List<String> ckmsGroups = Arrays.asList("tsdb.headless");
//        YKeyKeyClient client =
//            new YKeyKeyClient.YKeyKeyClientBuilder()
//                .environment(YKeyKeyEnvironment.corp)
//                .ztsClient(ztsClient)
//                .build();
//        keyReader = new CKMSReader(client, ckmsGroups);
      }

      dbUtil = new DBUtil(createMysqlDataSource(config.dbConfig));
      baseUrl = "https://" + env + "-config.opentsdbtsdb.net";

      String value = createOktaCookie(regularMember);
      cookieHeader = new Header("Cookie", value);

      StringBuilder unAuthorizedcookie = new StringBuilder();
      unAuthorizedcookie.append("okta_it=");
//      String unAuthorizedtoken =
//          DebugOktaFilter.createToken(
//              unauthorizedMember, unauthorizedMember.split("\\.")[1], "it", "integration test");
//      unAuthorizedcookie.append(unAuthorizedtoken);
      unauthorizedCookieHeader = new Header("Cookie", unAuthorizedcookie.toString());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

//  @NotNull
  protected static String createOktaCookie(String userId) {
    StringBuilder cookie = new StringBuilder();
    cookie.append("okta_it=");
//    String token =
//        DebugOktaFilter.createToken(userId, userId.split("\\.")[1], "it", "integration test");
//    cookie.append(token);
    return cookie.toString();
  }

  @BeforeAll
  public void beforeClass() throws Exception {
    // always clean up first
    cleanUpDBAndCreateUser();

    md5 = MessageDigest.getInstance("md5");
    sha256 = MessageDigest.getInstance("SHA-256");
    endPoint = baseUrl + "/api/v1/" + getUri();
    timestamp = new Timestamp(System.currentTimeMillis());
  }

  protected abstract String getUri();

  protected void cleanUpDBAndCreateUser() {
    cleanUpDB();
    storeHeadlessUser();
  }

  protected void cleanUpDB() {
    cascadeDeleteAlert();
    cascadeDeleteDashboard();
    cascadeDeleteNamespace();
    cascadeDeleteSnapshot();
    cascadeDeleteUser();
    deleteBcp();
  }

  protected void cascadeDeleteSnapshot() {
    dbUtil.clearTable("activity");
    dbUtil.clearTable("content_history");
    dbUtil.clearTable("snapshot");
    dbUtil.clearTable("content");
  }

  protected void cascadeDeleteNamespace() {
    dbUtil.clearTable("namespace_follower");
    dbUtil.clearTable("namespace_member");
    dbUtil.clearTable("snooze");
    dbUtil.clearTable("namespace");
    cascadeDeleteDashboard();
  }

  protected void cascadeDeleteDashboard() {
    dbUtil.clearTable("folder_activity");
    dbUtil.clearTable("favorite_folder");
    dbUtil.clearTable("folder_history");
    dbUtil.clearTable("folder");
    cascadeDeleteContent();
  }

  protected void cascadeDeleteContent() {
    dbUtil.clearTable("content_history");
    dbUtil.clearTable("snapshot");
    dbUtil.clearTable("content");
  }

  protected void cascadeDeleteAlert() {
    cascadeDeleteContact();
    dbUtil.clearTable("alert");
  }

  protected void cascadeDeleteSnooze() {
    cascadeDeleteAlert();
    dbUtil.clearTable("snooze");
  }

  protected void cascadeDeleteContact() {
    dbUtil.clearTable("alert_contact");
    dbUtil.clearTable("contact");
  }

  protected void deleteBcp() {
    dbUtil.clearTable("bcp");
  }

  protected void cascadeDeleteUser() {
    dbUtil.clearTable("user");
  }

  private static MysqlDataSource createMysqlDataSource(DBConfig dbConfig) throws KeyException {
    String dbUsername = dbConfig.dbUsername;
    String dbPassword = keyReader.getKey(dbConfig.dbKey);
    String dbName = dbConfig.dbName;
    String dbRWUrl = dbConfig.dbRWUrl;
    Map<String, String> jdbcPropertiesMap = dbConfig.jdbcProperties;

    String jdbcProperties = "";
    if (jdbcPropertiesMap != null && !jdbcPropertiesMap.isEmpty()) {
      jdbcProperties = formatJdbcProperties(jdbcPropertiesMap);
    }

    String mysqlRWUrl = "jdbc:mysql://" + dbRWUrl + "/" + dbName + jdbcProperties;

    MysqlDataSource dataSource = new MysqlDataSource();
    dataSource.setUrl(mysqlRWUrl);
    dataSource.setUser(dbUsername);
    dataSource.setPassword(dbPassword);
    return dataSource;
  }

  protected void storeHeadlessUser() {
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    User authorizedUser =
        buildUser(regularMember, "Yamas Headless User1", User.CreationMode.onthefly, timestamp);
    dbUtil.insert(authorizedUser);

    User unauthorizedUser =
        buildUser(
            BaseIT.unauthorizedMember,
            "Yamas Headless User2",
            User.CreationMode.onthefly,
            timestamp);
    dbUtil.insert(unauthorizedUser);
  }

  protected User buildUser(
      String userId, String name, User.CreationMode creationMode, Timestamp timestamp) {
    User user = new User().setUserid(userId).setName(name).setCreationmode(creationMode);
    user.setEnabled(true);
    user.setUpdatedtime(timestamp);
    return user;
  }

  protected Integer createNamespaceWithDefaultTime(Namespace namespace, String member)
      throws IOException {
    final long currentTime = System.currentTimeMillis();
    namespace.setCreatedBy(member);
    namespace.setUpdatedBy(member);
    namespace.setMeta(new HashMap<>());
    namespace.setCreatedTime(new Timestamp(currentTime));
    namespace.setUpdatedTime(new Timestamp(currentTime));
    return dbUtil.insert(namespace);
  }

  protected Integer createNamespaceWithMember(Namespace namespace, String member)
      throws IOException {
    // create namespace and member
    final Integer namespaceId = createNamespaceWithDefaultTime(namespace, member);
    dbUtil.insertNamespaceMember(namespaceId, member);
    return namespaceId;
  }

  void createBcpRecord(String serviceName, String primaryColo, Timestamp failoverTime) {

    dbUtil.insertBcpService(serviceName, primaryColo, failoverTime, 0);
  }

  void createBcpRecord(String serviceName, String primaryColo, Timestamp failoverTime, int offset) {
    dbUtil.insertBcpService(serviceName, primaryColo, failoverTime, offset);
  }

  protected static Namespace getNamespace() {
    Namespace namespace = new Namespace();
    namespace.setName("Yamas");
    namespace.setAlias(slugify("YamasAlias"));
    namespace.setMeta(new HashMap(){{put(DH_TRACKS, Arrays.asList("YamasTrack"));}});
    namespace.setEnabled(true);
    return namespace;
  }

  protected int insertContact(Contact contact) throws IOException {
    return dbUtil.createContact(contact);
  }

  protected long insertAlert(Alert alert) throws IOException {
    return dbUtil.createAlert(alert);
  }

  protected Folder createUserHomeFolder(String userId, Timestamp timestamp) {
    String path = "/user/" + userId.substring(userId.indexOf(".") + 1);
    return createHomeFolder(userId, timestamp, path);
  }

  protected Folder createNamespaceHomeFolder(String alias, String createdBy, Timestamp timestamp) {
    String path = "/namespace/" + alias;
    return createHomeFolder(createdBy, timestamp, path);
  }

  private Folder createHomeFolder(String userId, Timestamp timestamp, String path) {
    String name = "Home";
    byte[] pathHash = this.md5.digest(path.getBytes());
    Folder folder = new Folder();
    buildFolder(name, path, pathHash, null, userId, timestamp, userId, timestamp, folder);
    return folder;
  }

  protected Folder createTrashFolder(Folder homeFolder) {
    String name = "Trash";
    return createFolder(name, homeFolder.getCreatedTime(), homeFolder);
  }

  protected File createDashboard(String name, byte[] contentId, Timestamp timestamp, Folder parentFolder) {
    File file = new File();
    buildFolder(name, timestamp, parentFolder, file);
    file.setContentid(contentId);
    return file;
  }

  protected Folder createFolder(String name, Timestamp timestamp, Folder parentFolder) {
    Folder folder = new Folder();
    buildFolder(name, timestamp, parentFolder, folder);
    return folder;
  }

  protected Content createContent(Object data, Timestamp timestamp, String createdBy) throws IOException {
    byte[] serialized = serialize(data).getBytes();
    byte[] sha2 = sha256.digest(serialized);
    Content content = new Content(sha2, compress(serialized));
    content.setCreatedtime(timestamp);
    content.setCreatedby(createdBy);
    return content;
  }

  protected FileHistory createFileHistory(File file) {
    FileHistory fileHistory = new FileHistory();
    fileHistory.setFileid(file.getId());
    fileHistory.setContentid(file.getContentid());
    fileHistory.setCreatedtime(file.getCreatedTime());
    return fileHistory;
  }

  private void buildFolder(String name, Timestamp timestamp, Folder parentFolder, Folder folder) {
    String slug = slugify(name);
    String path = parentFolder.getPath() + "/" + slug;
    byte[] pathHash = this.md5.digest(path.getBytes());
    buildFolder(
        name,
        path,
        pathHash,
        parentFolder.getPathHash(),
        parentFolder.getCreatedBy(),
        timestamp,
        parentFolder.getUpdatedBy(),
        timestamp,
        folder);
  }

  private void buildFolder(
      String name,
      String path,
      byte[] pathHash,
      byte[] parentPathHash,
      String cretedBy,
      Timestamp createdTime,
      String updatedBy,
      Timestamp updatedTime,
      Folder folder) {
    folder.setName(name);
    folder.setType(FolderType.DASHBOARD);
    folder.setPath(path);
    folder.setPathHash(pathHash);
    folder.setParentPathHash(parentPathHash);
    folder.setCreatedBy(cretedBy);
    folder.setUpdatedBy(updatedBy);
    folder.setCreatedTime(createdTime);
    folder.setUpdatedTime(updatedTime);
  }

  protected long roundToSecond(Timestamp timestamp) {
    return Math.round((((double) timestamp.getTime()) / 1000)) * 1000;
  }

  protected Favorite createFavoriteFolder(String userId, long folderId, Timestamp timestamp) {
    Favorite favorite = new Favorite();
    favorite.setUserId(userId);
    favorite.setFolderId(folderId);
    favorite.setCreatedTime(timestamp);
    return favorite;
  }

  protected Namespace createNamespace(String name, String track, String createdBy, Timestamp timestamp) {
    Namespace namespace = new Namespace();
    namespace.setName(name);
    namespace.setAlias(slugify(name));
    namespace.setMeta(new HashMap(){{put(DH_TRACKS, Arrays.asList(track));}});
    namespace.setEnabled(true);
    namespace.setCreatedBy(createdBy);
    namespace.setCreatedTime(timestamp);
    namespace.setUpdatedBy(createdBy);
    namespace.setUpdatedTime(timestamp);
    return namespace;
  }
}
