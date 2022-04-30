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

import com.oath.auth.KeyRefresher;
import com.oath.auth.KeyRefresherException;
import com.stumbleupon.async.Deferred;
import com.yahoo.athenz.zms.ZMSClient;
import com.yahoo.athenz.zts.ZTSClient;
import io.ultrabrew.metrics.util.Strings;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.Registry;
import net.opentsdb.core.TSDB;
import net.opentsdb.horizon.NamespaceCache;
import net.opentsdb.horizon.SharedJDBCPool;
import net.opentsdb.horizon.UserCache;
import net.opentsdb.horizon.fs.store.FolderStore;
import net.opentsdb.horizon.model.User;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HorizonConfigServices extends BaseTSDBPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(HorizonConfigServices.class);

  public static final String TYPE = "HorizonConfigServices";

  public static final String KEY_PREFIX = "horizon.services.";

  public static final String DB_KEY = "database.client.id";
  public static final String NAMESPACE_TTL = "cache.namespace.ttl";
  public static final String USER_TTL = "cache.user.ttl";

  public static final String ZTS_URL_KEY = "athenz.zts.url";
  public static final String ZMS_URL_KEY = "athenz.zms.url";
  public static final String ATHENZ_DOMAIN_KEY = "athenz.domain";
  public static final String ATHENZ_PRIVATE_KEY = "athenz.privatekey";
  public static final String ATHENZ_PUBLIC_KEY = "athenz.publickey";
  public static final String ATHENZ_TRUST_STORE_KEY = "athenz.truststore.path";
  public static final String ATHENZ_TRUST_STORE_PASS_KEY = "athenz.trustore.password.key";

  public static final String ACTIVITY_THREADS_KEY = "activity.job.threads";
  public static final String ADMIN_EMAIL_KEY = "admin.email.domain";

  protected ExecutorService executorService;

  protected NamespaceMemberStore namespaceMemberStore;
  protected NamespaceFollowerStore namespaceFollowerStore;
  protected NamespaceStore namespaceStore;
  protected FolderStore folderStore;
  protected UserStore userStore;
  protected ContactStore contactStore;
  protected AlertStore alertStore;
  protected SnoozeStore snoozeStore;
  protected ContentStore contentStore;
  protected ActivityStore activityStore;
  protected SnapshotStore snapshotStore;

  protected NamespaceCache namespaceCache;
  protected UserCache userCache;

  protected AuthService authService;
  protected NamespaceFollowerService namespaceFollowerService;
  protected NamespaceMemberService namespaceMemberService;
  protected NamespaceService namespaceService;
  protected DashboardService dashboardService;
  protected UserService userService;
  protected ContactService contactService;
  protected AlertService alertService;
  protected SnoozeService snoozeService;
  protected ContentService contentService;
  protected SnapshotService snapshotService;

  protected DashboardActivityJobScheduler jobScheduler;
  protected ActivityJobScheduler activityJobScheduler;

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
    this.id = id;

    registerConfigs(tsdb);

    final MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      LOG.error("Couldn't load the SHA-256 digest?", e);
      return Deferred.fromError(e);
    }

    final Configuration config = tsdb.getConfig();
    final String dbId = config.getString(getConfigKey(DB_KEY));
    final SharedJDBCPool dbPool = tsdb.getRegistry().getPlugin(SharedJDBCPool.class,
            dbId);
    if (dbPool == null) {
      LOG.error("Cannot instantiate the Horizon config services without a DB pool. " +
                      "No plugin found for ID {}",
              (dbId == null ? "default" : dbId));
    }

    executorService = Executors.newFixedThreadPool(config.getInt(getConfigKey(ACTIVITY_THREADS_KEY)));

    // stores
    namespaceMemberStore = new NamespaceMemberStore(dbPool.getRwDataSource(),
            dbPool.getRoDataSource());
    namespaceFollowerStore = new NamespaceFollowerStore(dbPool.getRwDataSource(),
            dbPool.getRoDataSource());
    namespaceStore = new NamespaceStore(dbPool.getRwDataSource(), dbPool.getRoDataSource());
    folderStore = new FolderStore(dbPool.getRwDataSource(), dbPool.getRoDataSource());
    userStore = new UserStore(dbPool.getRwDataSource(), dbPool.getRoDataSource());
    contactStore = new ContactStore(dbPool.getRwDataSource(), dbPool.getRoDataSource());
    alertStore = new AlertStore(dbPool.getRwDataSource(), dbPool.getRoDataSource());
    snoozeStore = new SnoozeStore(dbPool.getRwDataSource(), dbPool.getRoDataSource());
    contentStore = new ContentStore(dbPool.getRwDataSource(), dbPool.getRoDataSource());
    activityStore = new ActivityStore(dbPool.getRwDataSource(), dbPool.getRoDataSource());
    snapshotStore = new SnapshotStore(dbPool.getRwDataSource(), dbPool.getRoDataSource());

    namespaceCache = new NamespaceCache(config.getInt(getConfigKey(NAMESPACE_TTL)), namespaceStore);
    userCache = new UserCache(config.getInt(getConfigKey(USER_TTL)), userStore);

    // TODO - Temporary as we need to support no-auth, etc.
    ZTSClient ztsClient = null;
    ZMSClient zmsClient = null;
    if (!Strings.isNullOrEmpty(config.getString(getConfigKey(ATHENZ_PUBLIC_KEY)))) {
      final SSLContext sslContext;
      try {
        KeyRefresher keyRefresher = com.oath.auth.Utils.generateKeyRefresher(
                config.getString(getConfigKey(ATHENZ_TRUST_STORE_KEY)),
                config.getSecretString(config.getString(getConfigKey(ATHENZ_TRUST_STORE_PASS_KEY))),
                config.getString(getConfigKey(ATHENZ_PUBLIC_KEY)),
                config.getString(getConfigKey(ATHENZ_PRIVATE_KEY)));
        keyRefresher.startup(300);
        sslContext = com.oath.auth.Utils.buildSSLContext(
                keyRefresher.getKeyManagerProxy(), keyRefresher.getTrustManagerProxy());
      } catch (IOException e) {
        return Deferred.fromError(e);
      } catch (InterruptedException e) {
        return Deferred.fromError(e);
      } catch (KeyRefresherException e) {
        return Deferred.fromError(e);
      }

      ztsClient = new ZTSClient(config.getString(getConfigKey(ZTS_URL_KEY)), sslContext);
      zmsClient = new ZMSClient(config.getString(getConfigKey(ZMS_URL_KEY)), sslContext);

      new AuthService(namespaceMemberStore, ztsClient,
              config.getString(getConfigKey(ATHENZ_DOMAIN_KEY)));
    } else {
      new AuthService(namespaceMemberStore, null, null);
    }

    namespaceFollowerService =
            new NamespaceFollowerService(namespaceFollowerStore, authService, namespaceCache);
    namespaceMemberService =
            new NamespaceMemberService(
                    namespaceMemberStore,
                    namespaceFollowerStore,
                    authService,
                    ztsClient,
                    zmsClient,
                    namespaceCache,
                    userCache);
    namespaceService =
            new NamespaceService(
                    namespaceStore,
                    namespaceMemberService,
                    namespaceFollowerService,
                    folderStore,
                    authService);

    jobScheduler =
            new DashboardActivityJobScheduler(folderStore, executorService);
    dashboardService =
            new DashboardService(
                    folderStore,
                    namespaceMemberService,
                    namespaceFollowerStore,
                    namespaceCache,
                    authService,
                    userStore,
                    digest,
                    jobScheduler);

    userService = new UserService(userStore, folderStore);
    contactService =
            new ContactService(
                    contactStore,
                    authService,
                    namespaceCache,
                    namespaceMemberService,
                    config.getString(getConfigKey(ADMIN_EMAIL_KEY)));
    alertService =
            new AlertService(alertStore, authService, namespaceCache, contactStore);
    snoozeService =
            new SnoozeService(snoozeStore, authService, namespaceCache, contactStore);
    contentService = new ContentService(digest, contentStore);
    activityJobScheduler =
            new ActivityJobScheduler(activityStore, executorService);
    snapshotService =
            new SnapshotService(
                    snapshotStore, contentService, folderStore, alertStore, activityJobScheduler);

    // register as shared objects for the resources to pick up.
    final Registry registry = tsdb.getRegistry();
    registry.registerSharedObject(NamespaceService.SO_SERVICE, namespaceService);
    registry.registerSharedObject(NamespaceMemberService.SO_SERVICE, namespaceMemberService);
    registry.registerSharedObject(NamespaceFollowerService.SO_SERVICE, namespaceFollowerService);

    registry.registerSharedObject(DashboardService.SO_SERVICE, dashboardService);

    registry.registerSharedObject(UserService.SO_SERVICE, userService);

    registry.registerSharedObject(ContactService.SO_SERVICE, contactService);
    registry.registerSharedObject(NamespaceCache.SO_NAMESPACE_CACHE, namespaceCache);

    registry.registerSharedObject(AlertService.SO_SERVICE, alertService);
    registry.registerSharedObject(SnoozeService.SO_SERVICE, snoozeService);
    registry.registerSharedObject(ContentService.SO_SERVICE, contentService);
    registry.registerSharedObject(SnapshotService.SO_SERVICE, snapshotService);

    // super ugly hack to initialize a default user for H2.
    if (tsdb.getConfig().hasProperty("horizon.config.h2.initialize") &&
        tsdb.getConfig().getBoolean("horizon.config.h2.initialize")) {
      try {
        LOG.info("Attempting to create the default 'noauth' user...");
        User user = new User()
            .setUserid("user.noauth")
            .setName("Default User")
            .setEnabled(true)
            .setCreationmode(User.CreationMode.onthefly);
        userService.create(user, "OpenTSDB");
        LOG.info("Successfully created the 'noauth' user.");

      } catch (Exception e) {
        LOG.error("Failed to create user", e);
        return Deferred.fromError(e);
      }
    }

    return Deferred.fromResult(null);
  }

  @Override
  public String type() {
    return TYPE;
  }

  private void registerConfigs(final TSDB tsdb) {
    // no config key as we can only have one API per
    final Configuration config = tsdb.getConfig();
    if (!config.hasProperty(getConfigKey(DB_KEY))) {
      config.register(getConfigKey(DB_KEY), null, false,
              "The ID of the shared database connection plugin. May be null for the default.");
    }
    if (!config.hasProperty(getConfigKey(NAMESPACE_TTL))) {
      config.register(getConfigKey(NAMESPACE_TTL), 300, false,
              "The time to live for an entry in the namespace cache in seconds.");
    }
    if (!config.hasProperty(getConfigKey(USER_TTL))) {
      config.register(getConfigKey(USER_TTL), 300, false,
              "The time to live for an entry in the user cache in seconds.");
    }

    if (!config.hasProperty(getConfigKey(ZTS_URL_KEY))) {
      config.register(getConfigKey(ZTS_URL_KEY), null, false,
              "TODO");
    }
    if (!config.hasProperty(getConfigKey(ZMS_URL_KEY))) {
      config.register(getConfigKey(ZMS_URL_KEY), null, false,
              "TODO");
    }
    if (!config.hasProperty(getConfigKey(ATHENZ_DOMAIN_KEY))) {
      config.register(getConfigKey(ATHENZ_DOMAIN_KEY), null, false,
              "TODO");
    }
    if (!config.hasProperty(getConfigKey(ATHENZ_PRIVATE_KEY))) {
      config.register(getConfigKey(ATHENZ_PRIVATE_KEY), null, false,
              "TODO");
    }
    if (!config.hasProperty(getConfigKey(ATHENZ_PUBLIC_KEY))) {
      config.register(getConfigKey(ATHENZ_PUBLIC_KEY), null, false,
              "TODO");
    }
    if (!config.hasProperty(getConfigKey(ATHENZ_TRUST_STORE_KEY))) {
      config.register(getConfigKey(ATHENZ_TRUST_STORE_KEY), null, false,
              "TODO");
    }
    if (!config.hasProperty(getConfigKey(ATHENZ_TRUST_STORE_PASS_KEY))) {
      config.register(getConfigKey(ATHENZ_TRUST_STORE_PASS_KEY), null, false,
              "TODO");
    }

    if (!config.hasProperty(getConfigKey(ACTIVITY_THREADS_KEY))) {
      config.register(getConfigKey(ACTIVITY_THREADS_KEY), 1, false,
              "How many threads to run for processing user activity.");
    }
    if (!config.hasProperty(getConfigKey(ADMIN_EMAIL_KEY))) {
      config.register(getConfigKey(ADMIN_EMAIL_KEY), "@opentsdb.net", false,
              "The domain to use for sending notifications.");
    }
  }

  private String getConfigKey(final String suffix) {
    if (id == null || id == TYPE) { // yes, same addy here.
      return KEY_PREFIX + suffix;
    } else {
      return KEY_PREFIX + id + "." + suffix;
    }
  }
}
