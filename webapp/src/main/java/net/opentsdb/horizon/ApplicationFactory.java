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

package net.opentsdb.horizon;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.oath.auth.KeyRefresher;
import com.oath.auth.KeyRefresherException;
import com.yahoo.athenz.zms.ZMSClient;
import com.yahoo.athenz.zts.ZTSClient;
import io.swagger.jaxrs.config.BeanConfig;
import net.opentsdb.horizon.config.ApplicationConfig;
import net.opentsdb.horizon.config.Config;
import net.opentsdb.horizon.config.DBConfig;
import net.opentsdb.horizon.config.ServerConfig;
import net.opentsdb.horizon.ext.ResourceExtender;
import net.opentsdb.horizon.fs.store.FolderStore;
import net.opentsdb.horizon.resource.AlertResource;
import net.opentsdb.horizon.resource.ContactsResource;
import net.opentsdb.horizon.resource.DashboardResource;
import net.opentsdb.horizon.resource.NamespaceAlertResource;
import net.opentsdb.horizon.resource.NamespaceResource;
import net.opentsdb.horizon.resource.NamespaceSnoozeResource;
import net.opentsdb.horizon.resource.OktaResource;
import net.opentsdb.horizon.resource.SnapshotResource;
import net.opentsdb.horizon.resource.SnoozeResource;
import net.opentsdb.horizon.resource.UserResource;
import net.opentsdb.horizon.secrets.KeyReader;
import net.opentsdb.horizon.secrets.KeyReaderFactory;
import net.opentsdb.horizon.service.ActivityJobScheduler;
import net.opentsdb.horizon.service.AlertService;
import net.opentsdb.horizon.service.AuthService;
import net.opentsdb.horizon.service.ContactService;
import net.opentsdb.horizon.service.ContentService;
import net.opentsdb.horizon.service.DashboardActivityJobScheduler;
import net.opentsdb.horizon.service.DashboardService;
import net.opentsdb.horizon.service.NamespaceFollowerService;
import net.opentsdb.horizon.service.NamespaceMemberService;
import net.opentsdb.horizon.service.NamespaceService;
import net.opentsdb.horizon.service.SnapshotService;
import net.opentsdb.horizon.service.SnoozeService;
import net.opentsdb.horizon.service.UserService;
import net.opentsdb.horizon.store.ActivityStore;
import net.opentsdb.horizon.store.AlertStore;
import net.opentsdb.horizon.store.ContactStore;
import net.opentsdb.horizon.store.ContentStore;
import net.opentsdb.horizon.store.NamespaceFollowerStore;
import net.opentsdb.horizon.store.NamespaceMemberStore;
import net.opentsdb.horizon.store.NamespaceStore;
import net.opentsdb.horizon.store.SnapshotStore;
import net.opentsdb.horizon.store.SnoozeStore;
import net.opentsdb.horizon.store.UserStore;

import javax.net.ssl.SSLContext;
import javax.ws.rs.core.Application;
import java.io.IOException;
import java.security.MessageDigest;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.oath.auth.KeyRefresher.DEFAULT_RETRY_CHECK_FREQUENCY;
import static net.opentsdb.horizon.Registry.AUTH_SERVICE;
import static net.opentsdb.horizon.Registry.KEYREADER;
import static net.opentsdb.horizon.Registry.NAMESPACE_CACHE;
import static net.opentsdb.horizon.Registry.NAMESPACE_MEMBER_SERVICE;
import static net.opentsdb.horizon.Registry.NAMESPACE_SERVICE;
import static net.opentsdb.horizon.Registry.RO_DATASOURCE;
import static net.opentsdb.horizon.Registry.RW_DATASOURCE;
import static net.opentsdb.horizon.Registry.USER_SERVICE;
import static net.opentsdb.horizon.Utils.loadClass;
import static net.opentsdb.horizon.config.ApplicationConfig.ATHENZ_PRIVATE_KEY;
import static net.opentsdb.horizon.config.ApplicationConfig.ATHENZ_PUBLIC_KEY;
import static net.opentsdb.horizon.config.ApplicationConfig.ATHENZ_SSLCONTEXT;
import static net.opentsdb.horizon.config.ApplicationConfig.ATHENZ_TRUSTORE;
import static net.opentsdb.horizon.config.ApplicationConfig.ATHENZ_TRUSTORE_PASSWORD;

public class ApplicationFactory {

  private Config config;

  SSLContext athenzSSlContext;

  public ApplicationFactory(Config config) throws Exception {
    this.config = config;
    ApplicationConfig applicationConfig = config.applicationConfig;
    Map<String, Object> appParams = applicationConfig.initParams;
    if (appParams != null) {
      String athensPrivateKey = (String) appParams.get(ATHENZ_PRIVATE_KEY);
      String athensPublicKey = (String) appParams.get(ATHENZ_PUBLIC_KEY);
      String athensTrustStore = (String) appParams.get(ATHENZ_TRUSTORE);
      String athensTrustStorePasswrod = (String) appParams.get(ATHENZ_TRUSTORE_PASSWORD);
      if (athensPrivateKey != null
          && athensPublicKey != null
          && athensTrustStore != null
          && athensTrustStorePasswrod != null) {
        this.athenzSSlContext =
            buildSSLContext(
                athensPrivateKey,
                athensPublicKey,
                athensTrustStore,
                athensTrustStorePasswrod,
                DEFAULT_RETRY_CHECK_FREQUENCY);
      }
    }
  }

  private SSLContext buildSSLContext(
      String athensPrivateKey,
      String athensPublicKey,
      String trustStore,
      String trustStorePassword,
      int refreshIntervalMillis)
      throws KeyRefresherException, IOException, InterruptedException {
    KeyRefresher keyRefresher =
        com.oath.auth.Utils.generateKeyRefresher(
            trustStore, trustStorePassword, athensPublicKey, athensPrivateKey);
    keyRefresher.startup(refreshIntervalMillis);
    return com.oath.auth.Utils.buildSSLContext(
        keyRefresher.getKeyManagerProxy(), keyRefresher.getTrustManagerProxy());
  }

  public Application buildApplication() throws Exception {
    final Set<Object> singletons = new HashSet<>();
    ApplicationConfig applicationConfig = config.applicationConfig;
    Map<String, Object> appParams = applicationConfig.initParams;
    if (appParams == null) {
      appParams = new HashMap<>();
    }
    appParams.put(ATHENZ_SSLCONTEXT, athenzSSlContext);

    String keyReaderFactoryClassName = applicationConfig.keyReaderFactoryClassName;
    KeyReaderFactory keyReaderFactory =
        ((Class<? extends KeyReaderFactory>) loadClass(keyReaderFactoryClassName))
            .getDeclaredConstructor()
            .newInstance();
    KeyReader keyReader = keyReaderFactory.createKeyReader(appParams);

    final DBConfig dbConfig = config.dbConfig;
    String dbUsername = dbConfig.dbUsername;
    String dbPassword = keyReader.getKey(dbConfig.dbKey);
    String dbName = dbConfig.dbName;
    String dbRWUrl = dbConfig.dbRWUrl;
    String dbROUrl = dbConfig.dbROUrl;
    Map<String, String> jdbcPropertiesMap = dbConfig.jdbcProperties;

    String jdbcProperties = "";
    if (jdbcPropertiesMap != null && !jdbcPropertiesMap.isEmpty()) {
      jdbcProperties = formatJdbcProperties(jdbcPropertiesMap);
    }

    String mysqlROUrl = "jdbc:mysql://" + dbROUrl + "/" + dbName + jdbcProperties;
    String mysqlRWUrl = "jdbc:mysql://" + dbRWUrl + "/" + dbName + jdbcProperties;

    ComboPooledDataSource rwDataSource =
        createPooledDataSource(dbUsername, dbPassword, mysqlRWUrl, dbConfig);
    ComboPooledDataSource roDataSource =
        createPooledDataSource(dbUsername, dbPassword, mysqlROUrl, dbConfig);

    MessageDigest digest = MessageDigest.getInstance("SHA-256");

    NamespaceMemberStore namespaceMemberStore =
        new NamespaceMemberStore(rwDataSource, roDataSource);
    NamespaceFollowerStore namespaceFollowerStore =
        new NamespaceFollowerStore(rwDataSource, roDataSource);
    NamespaceStore namespaceStore = new NamespaceStore(rwDataSource, roDataSource);

    FolderStore folderStore = new FolderStore(rwDataSource, roDataSource);
    UserStore userStore = new UserStore(rwDataSource, roDataSource);

    NamespaceCache namespaceCache =
        new NamespaceCache(applicationConfig.cacheConfig, namespaceStore);
    UserCache userCache = new UserCache(applicationConfig.cacheConfig, userStore);

    String ztsUrl = (String) appParams.get("ztsUrl");
    String zmsUrl = (String) appParams.get("zmsUrl");
    String athensDomain = (String) appParams.get("athensDomain");

    ZTSClient ztsClient = null;
    ZMSClient zmsClient = null;
    if (athenzSSlContext != null) {
      ztsClient = new ZTSClient(ztsUrl, athenzSSlContext);
      zmsClient = new ZMSClient(zmsUrl, athenzSSlContext);
    }

    String athenzProviderService = (String) appParams.get("athenzService");
    String athenzServiceProviderDomain = (String) appParams.get("athenzServiceProviderDomain");

    AuthService authService = new AuthService(namespaceMemberStore, ztsClient, athensDomain, athenzProviderService, athenzServiceProviderDomain);
    NamespaceFollowerService namespaceFollowerService =
        new NamespaceFollowerService(namespaceFollowerStore, authService, namespaceCache);
    NamespaceMemberService namespaceMemberService =
        new NamespaceMemberService(
            namespaceMemberStore,
            namespaceFollowerStore,
            authService,
            ztsClient,
            zmsClient,
            namespaceCache,
            userCache,
            athenzProviderService,
            athenzServiceProviderDomain);
    NamespaceService namespaceService =
        new NamespaceService(
            namespaceStore,
            namespaceMemberService,
            namespaceFollowerService,
            folderStore,
            authService);

    NamespaceResource namespaceResource =
        new NamespaceResource(namespaceService, namespaceMemberService, namespaceFollowerService);

    ExecutorService executorService = Executors.newFixedThreadPool(5);
    DashboardActivityJobScheduler jobScheduler =
        new DashboardActivityJobScheduler(folderStore, executorService);

    DashboardService dashboardService =
        new DashboardService(
            folderStore,
            namespaceMemberService,
            namespaceFollowerStore,
            namespaceCache,
            userCache,
            authService,
            userStore,
            digest,
            jobScheduler);
    DashboardResource dashboardResource = new DashboardResource(dashboardService);

    UserService userService = new UserService(userStore, folderStore);
    UserResource userResource = new UserResource(userService);

    ContactStore contactStore = new ContactStore(rwDataSource, roDataSource);
    String adminEmailDomain = applicationConfig.adminEmailDomain;
    ContactService contactService =
        new ContactService(
            contactStore, authService, namespaceCache, namespaceMemberService, adminEmailDomain);
    ContactsResource contactsResource = new ContactsResource(contactService, namespaceCache);

    AlertStore alertStore = new AlertStore(rwDataSource, roDataSource);
    AlertService alertService =
        new AlertService(alertStore, authService, namespaceCache, contactStore);
    NamespaceAlertResource namespaceAlertResource =
        new NamespaceAlertResource(alertService, namespaceCache);
    AlertResource alertResource = new AlertResource(alertService);

    SnoozeStore snoozeStore = new SnoozeStore(rwDataSource, roDataSource);
    SnoozeService snoozeService =
        new SnoozeService(snoozeStore, authService, namespaceCache, contactStore);
    NamespaceSnoozeResource namespaceSnoozeResource =
        new NamespaceSnoozeResource(snoozeService, namespaceCache);
    SnoozeResource snoozeResource = new SnoozeResource(snoozeService);

    ContentStore contentStore = new ContentStore(rwDataSource, roDataSource);
    ContentService contentService = new ContentService(digest, contentStore);

    ActivityStore activityStore = new ActivityStore(rwDataSource, roDataSource);
    ActivityJobScheduler activityJobScheduler =
        new ActivityJobScheduler(activityStore, executorService);

    SnapshotStore snapshotStore = new SnapshotStore(rwDataSource, roDataSource);
    SnapshotService snapshotService =
        new SnapshotService(
            snapshotStore, contentService, folderStore, alertStore, activityJobScheduler);
    SnapshotResource snapshotResource = new SnapshotResource(snapshotService);

    // add all resource classes
    singletons.add(namespaceResource);
    singletons.add(userResource);
    singletons.add(dashboardResource);
    singletons.add(new OktaResource());
    singletons.add(contactsResource);
    singletons.add(namespaceAlertResource);
    singletons.add(alertResource);
    singletons.add(namespaceSnoozeResource);
    singletons.add(snoozeResource);
    singletons.add(snapshotResource);

    String resourceExtenderClassName = applicationConfig.resourceExtenderClassName;
    if (resourceExtenderClassName != null && !resourceExtenderClassName.isEmpty()) {

      final Map<String, Object> objectMap = new HashMap<>();
      objectMap.put(KEYREADER, keyReader);
      objectMap.put(RW_DATASOURCE, rwDataSource);
      objectMap.put(RO_DATASOURCE, roDataSource);
      objectMap.put(ATHENZ_SSLCONTEXT, athenzSSlContext);
      objectMap.put(NAMESPACE_SERVICE, namespaceService);
      objectMap.put(NAMESPACE_MEMBER_SERVICE, namespaceMemberService);
      objectMap.put(NAMESPACE_CACHE, namespaceCache);
      objectMap.put(USER_SERVICE, userService);
      objectMap.put(AUTH_SERVICE, authService);

      net.opentsdb.horizon.Registry registry = new net.opentsdb.horizon.Registry(objectMap);

      ResourceExtender resourceExtender =
          ((Class<? extends ResourceExtender>) loadClass(resourceExtenderClassName))
              .getDeclaredConstructor(net.opentsdb.horizon.Registry.class)
              .newInstance(registry);
      resourceExtender.createResource(appParams);
      singletons.addAll(resourceExtender.getSingletons());
    }

    return new SingletonApplication(singletons, config.serverConfig);
  }

  public static String formatJdbcProperties(Map<String, String> jdbcPropertiesMap) {
    StringBuilder builder = new StringBuilder();
    boolean firstEntry = true;
    for (Map.Entry<String, String> entry : jdbcPropertiesMap.entrySet()) {
      builder.append(firstEntry ? "?" : "&");
      if (firstEntry) {
        firstEntry = false;
      }
      builder.append(entry.getKey()).append("=").append(entry.getValue());
    }
    return builder.toString();
  }

  private ComboPooledDataSource createPooledDataSource(
      String dbUsername, String dbKey, String dbUrl, DBConfig dbConfig) throws SQLException {
    ComboPooledDataSource dataSource = new ComboPooledDataSource();
    dataSource.setJdbcUrl(dbUrl);
    dataSource.setUser(dbUsername);
    dataSource.setPassword(dbKey);

    dataSource.setMinPoolSize(dbConfig.c3p0MinPoolSize);
    dataSource.setMaxPoolSize(dbConfig.c3p0MaxPoolSize);
    dataSource.setLoginTimeout(dbConfig.c3p0LoginTimeout);
    dataSource.setMaxStatements(dbConfig.c3p0MaxStatements);
    dataSource.setMaxStatementsPerConnection(dbConfig.c3p0MaxStatementsPerConnection);
    dataSource.setMaxConnectionAge(dbConfig.c3p0MaxConnectionAge);
    dataSource.setMaxIdleTime(dbConfig.c3p0MaxIdleTime);
    dataSource.setMaxIdleTimeExcessConnections(dbConfig.c3p0MaxIdleTimeExcessConnections);
    dataSource.setIdleConnectionTestPeriod(dbConfig.c3p0IdleConnectionTestPeriod);
    dataSource.setTestConnectionOnCheckin(dbConfig.c3p0TestConnectionOnCheckin);
    dataSource.setTestConnectionOnCheckout(dbConfig.c3p0TestConnectionOnCheckout);
    return dataSource;
  }

  private class SingletonApplication extends Application {

    private final Set<Object> singletons;
    private ServerConfig serverConfig;

    SingletonApplication(Set<Object> singletons, ServerConfig serverConfig) {
      this.singletons = Collections.unmodifiableSet(singletons);
      this.serverConfig = serverConfig;

      if (serverConfig.swaggerEnabled) {
        BeanConfig beanConfig = new BeanConfig();
        beanConfig.setTitle(serverConfig.swaggerTitle);
        beanConfig.setVersion(serverConfig.swaggerVersion);
        beanConfig.setBasePath(serverConfig.swaggerBasePath);
        beanConfig.setHost(serverConfig.swaggerHost + ":" + serverConfig.port);
        beanConfig.setResourcePackage(serverConfig.swaggerResourcePackage);
        beanConfig.setPrettyPrint(true);
        beanConfig.setScan(true);
      }
    }

    @Override
    public Set<Object> getSingletons() {
      return singletons;
    }

    @Override
    public Set<Class<?>> getClasses() {
      if (serverConfig.swaggerEnabled) {
        HashSet<Class<?>> set = new HashSet<>();
        set.add(io.swagger.jaxrs.listing.ApiListingResource.class);
        set.add(io.swagger.jaxrs.listing.SwaggerSerializers.class);
        return set;
      } else {
        return super.getClasses();
      }
    }
  }
}
