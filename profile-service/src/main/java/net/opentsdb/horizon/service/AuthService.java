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

import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.model.User;
import net.opentsdb.horizon.store.NamespaceMemberStore;
import com.yahoo.athenz.zts.ZTSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;
import java.util.stream.Collectors;

import static net.opentsdb.horizon.profile.Utils.getAthensDomain;

public class AuthService {

  private static final Logger LOGGER = LoggerFactory.getLogger(AuthService.class);

  public static final String PROVIDER_DOMAIN = "tsdb.property";
  public static final String PROVIDER_SERVICE = "monitoring";
  public static final String ACCESS_ROLE_FORMAT = "%s.tenant.%s.res_group.namespace_%s.access";

  private final NamespaceMemberStore memberStore;
  private final ZTSClient ztsClient;
  private final String athensDomain;

  public AuthService(
      final NamespaceMemberStore memberStore,
      final ZTSClient ztsClient,
      final String athensDomain) {
    this.memberStore = memberStore;
    this.ztsClient = ztsClient;
    this.athensDomain = athensDomain;
  }

  public boolean isSuperAdmin(String principal) {
    if (ztsClient == null) {
      return false;
    }
    return ztsClient.getAccess(athensDomain, "superadmin", principal).granted;
  }

  public boolean authorize(Namespace namespace, String principal) throws SQLException {
    String tenantDomain = getAthensDomain(namespace);
    boolean authorized;
    if (tenantDomain == null) {
      authorized = isMember(namespace.getId(), principal);
    } else {
      authorized = checkAccess(namespace.getAlias(), tenantDomain, principal);
    }
    if (!authorized) {
      authorized = isSuperAdmin(principal);
      if (authorized) {
        LOGGER.info("Used super admin privilege: " + principal);
      }
    }
    return authorized;
  }

  private boolean isMember(int namespaceId, String principal) throws SQLException {
    try (Connection connection = memberStore.getReadOnlyConnection()) {
      final Set<String> memberSet =
          memberStore.getNamespaceMembers(namespaceId, connection).stream()
              .map(User::getUserid)
              .collect(Collectors.toSet());
      return memberSet.contains(principal);
    }
  }

  private boolean checkAccess(
      final String namespaceAlias, final String tenantDomain, final String principal) {
    String roleName =
        String.format(ACCESS_ROLE_FORMAT, PROVIDER_SERVICE, tenantDomain, namespaceAlias);
    return ztsClient.getAccess(PROVIDER_DOMAIN, roleName, principal).granted;
  }
}
