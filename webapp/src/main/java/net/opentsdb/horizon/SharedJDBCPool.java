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

import com.google.common.collect.Maps;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.stumbleupon.async.Deferred;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class SharedJDBCPool extends BaseTSDBPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(SharedJDBCPool.class);

  public static final String TYPE = "SharedJDBCPool";

  public static final String KEY_PREFIX = "jdbcpool.";

  public static final String RO_USER_KEY = "read.user";
  public static final String RO_PASS_KEY_KEY = "read.secret.key";
  public static final String RO_URL_KEY = "read.url";

  public static final String RW_USER_KEY = "write.user";
  public static final String RW_PASS_KEY_KEY = "write.secret.key";
  public static final String RW_URL_KEY = "write.url";

  public static final String DB_KEY = "database.name";
  public static final String JDBC_PROPS_KEY = "jdbc.properties";

  public static final String MIN_KEY = "pool.min";
  public static final String MAX_KEY = "pool.max";
  public static final String AGE_KEY = "pool.connection.age.max";
  public static final String LOGIN_TIMEOUT_KEY = "pool.login.timeout";
  public static final String MAX_STATEMENTS_KEY = "pool.statements.max";
  public static final String CONN_STATEMENTS_KEY = "pool.statements.perConnection";
  public static final String IDLE_KEY = "pool.idle.time";
  public static final String IDLE_PERIOD_KEY = "pool.idle.test.period";
  public static final String IDLE_EXCESS_KEY = "pool.idle.excess";
  public static final String CHEKIN_KEY = "pool.test.checkin";
  public static final String CHEKOUT_KEY = "pool.test.checkout";

  protected ComboPooledDataSource rwDataSource;
  protected ComboPooledDataSource roDataSource;

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
    this.id = id;

    registerConfigs(tsdb);
    final Configuration config = tsdb.getConfig();

    try {
      roDataSource = createPooledDataSource(
              config.getString(getConfigKey(RO_USER_KEY)),
              config.getSecretString(config.getString(getConfigKey(RO_PASS_KEY_KEY))),
              config.getString(getConfigKey(RO_URL_KEY)));

      rwDataSource = createPooledDataSource(
              config.getString(getConfigKey(RW_USER_KEY)),
              config.getSecretString(config.getString(getConfigKey(RW_PASS_KEY_KEY))),
              config.getString(getConfigKey(RW_URL_KEY)));
    } catch (SQLException e) {
      LOG.error("Failed to initialize the shared MySQL pool", e);
      return Deferred.fromError(e);
    }

    return Deferred.fromResult(null);
  }

  public ComboPooledDataSource getRwDataSource() {
    return rwDataSource;
  }

  public ComboPooledDataSource getRoDataSource() {
    return roDataSource;
  }

  @Override
  public String type() {
    return TYPE;
  }

  private ComboPooledDataSource createPooledDataSource(final String dbUsername,
                                                       final String dbKey,
                                                       final String dbUrl) throws SQLException {
    ComboPooledDataSource dataSource = new ComboPooledDataSource();
    dataSource.setJdbcUrl(dbUrl);
    dataSource.setUser(dbUsername);
    dataSource.setPassword(dbKey);

    final Configuration config = tsdb.getConfig();

    dataSource.setMinPoolSize(config.getInt(getConfigKey(MIN_KEY)));
    dataSource.setMaxPoolSize(config.getInt(getConfigKey(MAX_KEY)));
    dataSource.setLoginTimeout(config.getInt(getConfigKey(LOGIN_TIMEOUT_KEY)));
    dataSource.setMaxStatements(config.getInt(getConfigKey(MAX_STATEMENTS_KEY)));
    dataSource.setMaxStatementsPerConnection(config.getInt(getConfigKey(CONN_STATEMENTS_KEY)));
    dataSource.setMaxConnectionAge(config.getInt(getConfigKey(AGE_KEY)));
    dataSource.setMaxIdleTime(config.getInt(getConfigKey(IDLE_KEY)));
    dataSource.setMaxIdleTimeExcessConnections(config.getInt(getConfigKey(IDLE_EXCESS_KEY)));
    dataSource.setIdleConnectionTestPeriod(config.getInt(getConfigKey(IDLE_PERIOD_KEY)));
    dataSource.setTestConnectionOnCheckin(config.getBoolean(getConfigKey(CHEKIN_KEY)));
    dataSource.setTestConnectionOnCheckout(config.getBoolean(getConfigKey(CHEKOUT_KEY)));
    return dataSource;
  }

  private void registerConfigs(final TSDB tsdb) {
    final Configuration config = tsdb.getConfig();
    if (!config.hasProperty(getConfigKey(RO_USER_KEY))) {
      config.register(getConfigKey(RO_USER_KEY), null, false,
              "The user name for the read only database connections.");
    }
    if (!config.hasProperty(getConfigKey(RO_PASS_KEY_KEY))) {
      config.register(getConfigKey(RO_PASS_KEY_KEY), null, false,
              "The key to use to fetch the password for the read only database connections.");
    }
    if (!config.hasProperty(getConfigKey(RO_URL_KEY))) {
      config.register(getConfigKey(RO_URL_KEY), "jdbc:mysql://localhost/configdb?serverTimezone=UTC", false,
              "The fully qualified URL to connect over JDBC to the read only database.");
    }

    if (!config.hasProperty(getConfigKey(RW_USER_KEY))) {
      config.register(getConfigKey(RW_USER_KEY), null, false,
              "The user name for the read/write database connections.");
    }
    if (!config.hasProperty(getConfigKey(RW_PASS_KEY_KEY))) {
      config.register(getConfigKey(RW_PASS_KEY_KEY), null, false,
              "The key to use to fetch the password for the read/write database connections.");
    }
    if (!config.hasProperty(getConfigKey(RW_URL_KEY))) {
      config.register(getConfigKey(RW_URL_KEY), "jdbc:mysql://localhost/configdb?serverTimezone=UTC", false,
              "The fully qualified URL to connect over JDBC to the read/write database.");
    }

    if (!config.hasProperty(getConfigKey(DB_KEY))) {
      config.register(getConfigKey(DB_KEY), "configdb", false,
              "The name of the database for both write and read only connections.");
    }
    if (!config.hasProperty(getConfigKey(JDBC_PROPS_KEY))) {
      config.register(ConfigurationEntrySchema.newBuilder()
              .setKey(getConfigKey(JDBC_PROPS_KEY))
              .setType(JSON.STRING_MAP_REFERENCE)
              .setDefaultValue(Maps.newHashMap())
              .setDescription("A map of JDBC configuration options.")
              .setSource(getClass().getName())
              .build());
    }

    if (!config.hasProperty(getConfigKey(MIN_KEY))) {
      config.register(getConfigKey(MIN_KEY), "1", false,
              "The minimum number of connections in each C3P0 pool.");
    }
    if (!config.hasProperty(getConfigKey(MAX_KEY))) {
      config.register(getConfigKey(MAX_KEY), "1", false,
              "The maximum number of connections in each C3P0 pool. 0 means no max?");
    }
    if (!config.hasProperty(getConfigKey(AGE_KEY))) {
      config.register(getConfigKey(AGE_KEY), "3600", false,
              "How long to keep a connection alive before closing and creating another.");
    }
    if (!config.hasProperty(getConfigKey(LOGIN_TIMEOUT_KEY))) {
      config.register(getConfigKey(LOGIN_TIMEOUT_KEY), "30", false,
              "The login timeout in seconds for a new connection.");
    }
    if (!config.hasProperty(getConfigKey(MAX_STATEMENTS_KEY))) {
      config.register(getConfigKey(MAX_STATEMENTS_KEY), "1024", false,
              "TODO.");
    }
    if (!config.hasProperty(getConfigKey(CONN_STATEMENTS_KEY))) {
      config.register(getConfigKey(CONN_STATEMENTS_KEY), "1024", false,
              "TODO.");
    }
    if (!config.hasProperty(getConfigKey(IDLE_KEY))) {
      config.register(getConfigKey(IDLE_KEY), "7200", false,
              "How long, in seconds, before terminating a pool connection when it hasn't handled any requests.");
    }
    if (!config.hasProperty(getConfigKey(IDLE_PERIOD_KEY))) {
      config.register(getConfigKey(IDLE_PERIOD_KEY), "60", false,
              "TODO");
    }
    if (!config.hasProperty(getConfigKey(IDLE_EXCESS_KEY))) {
      config.register(getConfigKey(IDLE_EXCESS_KEY), "0", false,
              "TODO");
    }
    if (!config.hasProperty(getConfigKey(CHEKIN_KEY))) {
      config.register(getConfigKey(CHEKIN_KEY), false, false,
              "TODO.");
    }
    if (!config.hasProperty(getConfigKey(CHEKOUT_KEY))) {
      config.register(getConfigKey(CHEKOUT_KEY), false, false,
              "TODO.");
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
