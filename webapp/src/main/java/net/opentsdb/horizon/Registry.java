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

import net.opentsdb.horizon.secrets.KeyReader;
import net.opentsdb.horizon.service.AuthService;
import net.opentsdb.horizon.service.NamespaceMemberService;
import net.opentsdb.horizon.service.NamespaceService;
import net.opentsdb.horizon.service.UserService;

import javax.net.ssl.SSLContext;
import javax.sql.DataSource;
import java.util.Map;

import static net.opentsdb.horizon.config.ApplicationConfig.ATHENZ_SSLCONTEXT;

public class Registry {

  public static final String KEYREADER = "keyReader";
  public static final String RO_DATASOURCE = "roDataSource";
  public static final String RW_DATASOURCE = "rwDataSource";
  public static final String NAMESPACE_SERVICE = "namespaceService";
  public static final String NAMESPACE_CACHE = "namespaceCache";
  public static final String NAMESPACE_MEMBER_SERVICE = "namespaceMemberService";
  public static final String USER_SERVICE = "userService";
  public static final String AUTH_SERVICE = "authService";

  final Map<String, Object> map;

  public Registry(final Map<String, Object> map) {
    this.map = map;
  }

  public KeyReader getKeyReader() {
    return get(KEYREADER);
  }

  public DataSource getRODataSource() {
    return get(RO_DATASOURCE);
  }

  public DataSource getRWDataSource() {
    return get(RW_DATASOURCE);
  }

  public SSLContext getAthenzSSLContext() {
    return get(ATHENZ_SSLCONTEXT);
  }

  public NamespaceService getNamespaceService() {
    return get(NAMESPACE_SERVICE);
  }

  public NamespaceCache getNamespaceCache() {
    return get(NAMESPACE_CACHE);
  }

  public NamespaceMemberService getNamespaceMemberService() {
    return get(NAMESPACE_MEMBER_SERVICE);
  }

  public UserService getUserService() {
    return get(USER_SERVICE);
  }

  public AuthService getAuthService() {
    return get(AUTH_SERVICE);
  }

  private <T> T get(String key) {
    return (T) map.get(key);
  }
}
