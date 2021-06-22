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

import net.opentsdb.horizon.NamespaceCache;
import net.opentsdb.horizon.UserCache;
import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.model.User;
import net.opentsdb.horizon.model.User.CreationMode;
import net.opentsdb.horizon.profile.Utils;
import net.opentsdb.horizon.store.NamespaceFollowerStore;
import net.opentsdb.horizon.store.NamespaceMemberStore;
import com.yahoo.athenz.zms.Role;
import com.yahoo.athenz.zms.RoleMember;
import com.yahoo.athenz.zms.ZMSClient;
import com.yahoo.athenz.zms.ZMSClientException;
import com.yahoo.athenz.zts.ZTSClient;
import com.yahoo.rdl.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotAllowedException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static net.opentsdb.horizon.profile.Utils.getAthensDomain;
import static net.opentsdb.horizon.profile.Utils.isAthensManaged;
import static net.opentsdb.horizon.profile.Utils.validateNamespace;
import static net.opentsdb.horizon.service.AuthService.ACCESS_ROLE_FORMAT;
import static net.opentsdb.horizon.service.AuthService.PROVIDER_DOMAIN;
import static net.opentsdb.horizon.service.AuthService.PROVIDER_SERVICE;
import static net.opentsdb.horizon.service.BaseService.internalServerError;

public class NamespaceMemberService {

  private static final Logger LOGGER = LoggerFactory.getLogger(NamespaceMemberService.class);

  private final NamespaceMemberStore memberStore;
  private final NamespaceFollowerStore followerStore;
  private final AuthService authService;
  private ZTSClient ztsClient;
  private ZMSClient zmsClient;
  private NamespaceCache namespaceCache;
  private UserCache userCache;

  public NamespaceMemberService(
      final NamespaceMemberStore memberStore,
      final NamespaceFollowerStore followerStore,
      final AuthService authService,
      final ZTSClient ztsClient,
      final ZMSClient zmsClient,
      final NamespaceCache namespaceCache,
      final UserCache userCache) {

    this.memberStore = memberStore;
    this.followerStore = followerStore;
    this.authService = authService;
    this.ztsClient = ztsClient;
    this.zmsClient = zmsClient;
    this.namespaceCache = namespaceCache;
    this.userCache = userCache;
  }

  public List<User> getNamespaceMember(final int namespaceId) {

    Namespace namespace;
    try {
      namespace = namespaceCache.getById(namespaceId);
    } catch (Exception e) {
      String message = "Error reading namespace with id: " + namespaceId;
      LOGGER.error(message, e);
      throw internalServerError(message);
    }
    validateNamespace(namespace, namespaceId);
    String tenantDomain = getAthensDomain(namespace);
    if (tenantDomain == null) {
      try (final Connection connection = memberStore.getReadOnlyConnection()) {
        return memberStore.getNamespaceMembers(namespaceId, connection);
      } catch (SQLException sqlException) {
        String message = "Error reading members for namespace: " + namespace.getName();
        LOGGER.error(message, sqlException);
        throw internalServerError(message);
      }
    } else {
      String roleName =
          String.format(ACCESS_ROLE_FORMAT, PROVIDER_SERVICE, tenantDomain, namespace.getAlias());
      Role role;
      try {
        role = zmsClient.getRole(PROVIDER_DOMAIN, roleName, false, true);
      } catch (ZMSClientException e) {
        String message = "Error reading members for namespace: " + namespace.getName();
        LOGGER.error(message, e);
        throw internalServerError(message);
      }

      List<RoleMember> roleMembers = role.getRoleMembers();
      List<String> userIds = new ArrayList<>();
      if (roleMembers != null) {
        for (RoleMember member : roleMembers) {
          Integer systemDisabled = member.getSystemDisabled();
          Timestamp expiration = member.getExpiration();
          boolean disabled = systemDisabled != null && systemDisabled > 0;
          boolean expired =
              expiration != null && (expiration.millis() <= System.currentTimeMillis());
          if (!disabled && !expired) {
            userIds.add(member.getMemberName());
          }
        }
      }

      List<User> users = Collections.emptyList();
      if (userIds != null) {
        users =
            userIds.stream()
                .map(
                    userId -> {
                      User user = userCache.getById(userId);
                      if (user == null) {
                        // user present in athens but, not present in tsdb
                        user = new User();
                        user.setUserid(userId);
                        user.setCreationmode(CreationMode.notcreated);
                      } else {
                        // strip out the meta fields
                        user.setCreationmode(null);
                        user.setUpdatedtime(null);
                      }
                      return user;
                    })
                .collect(Collectors.toList());
      }
      return users;
    }
  }

  public void addNamespaceMember(
      final int namespaceId, final List<String> memberIdList, String principal) {
    Namespace namespace;
    try {
      namespace = namespaceCache.getById(namespaceId);
    } catch (Exception e) {
      String message = "Error reading namespace with id: " + namespaceId;
      LOGGER.error(message, e);
      throw internalServerError(message);
    }
    validateNamespace(namespace, namespaceId);
    if (isAthensManaged(namespace)) {
      throw new NotAllowedException("Membership is managed in Athens");
    }

    try {
      if (!authService.authorize(namespace, principal)) {
        throw new ForbiddenException("Namespace admin only allowed to add members.");
      }
    } catch (SQLException e) {
      String message = "Error doing the authz check";
      LOGGER.error(message, e);
      throw internalServerError(message);
    }
    try (Connection connection = memberStore.getReadWriteConnection()) {
      try {
        addNamespaceMemberWithoutCommit(namespaceId, memberIdList, connection);
        memberStore.commit(connection);
      } catch (SQLException e) {
        memberStore.rollback(connection);
        throw e;
      }
    } catch (SQLException sqlException) {
      String message = "Error adding members to namespace: " + namespace.getName();
      LOGGER.error(message, sqlException);
      throw internalServerError(message);
    }
  }

  void addNamespaceMemberWithoutCommit(
      final int namespaceId, final List<String> memberIdList, Connection connection)
      throws SQLException {
    memberStore.addMembers(namespaceId, memberIdList, connection);
    followerStore.removeFollowers(namespaceId, memberIdList, connection);
  }

  void addNamespaceMember(
      final List<Integer> namespaceIds, final List<String> memberIds, final Connection connection)
      throws SQLException {
    memberStore.addMembers(namespaceIds, memberIds, connection);
    followerStore.removeFollowers(namespaceIds, memberIds, connection);
  }

  public void removeNamespaceMember(
      final int namespaceId, final List<String> memberIdList, String principal) {
    Namespace namespace;
    try {
      namespace = namespaceCache.getById(namespaceId);
    } catch (Exception e) {
      String message = "Error reading namespace with id: " + namespaceId;
      LOGGER.error(message, e);
      throw internalServerError(message);
    }
    validateNamespace(namespace, namespaceId);
    try {
      if (!authService.authorize(namespace, principal)) {
        throw new ForbiddenException("Namespace admin only allowed to remove member.");
      }
    } catch (SQLException e) {
      String message = "Error doing the authz check";
      LOGGER.error(message, e);
      throw internalServerError(message);
    }

    try (Connection connection = memberStore.getReadWriteConnection()) {
      try {
        memberStore.removeNamespaceMember(namespaceId, memberIdList, connection);
        memberStore.commit(connection);
      } catch (SQLException e) {
        memberStore.rollback(connection);
        throw e;
      }
    } catch (SQLException sqlException) {
      String message = "Error removing members from namespace: " + namespace.getName();
      LOGGER.error(message, sqlException);
      throw internalServerError(message);
    }
  }

  public void removeAllNamespaceMember(final int namespaceId, String principal) throws Exception {
    Namespace namespace = namespaceCache.getById(namespaceId);
    validateNamespace(namespace, namespaceId);
    if (!authService.authorize(namespace, principal)) {
      throw new ForbiddenException("Namespace admin only allowed to remove member.");
    }
    try (Connection conn = memberStore.getReadWriteConnection()) {
      try {
        memberStore.removeAllNamespaceMember(namespaceId, conn);
        memberStore.commit(conn);
      } catch (SQLException e) {
        memberStore.rollback(conn);
        throw e;
      }
    }
  }

  public List<Namespace> getNamespaces(String userId) {
    try (Connection connection = memberStore.getReadOnlyConnection()) {
      return getNamespaces(userId, connection);
    } catch (Exception e) {
      String message = "Error reading namespaces of user: " + userId;
      LOGGER.error(message, e);
      throw internalServerError(message);
    }
  }

  /** This api is package protected, to be used by other services initiating the jdbc transaction */
  List<Namespace> getNamespaces(String userId, Connection connection) {

    List<Namespace> namespaces;
    try {
      namespaces = memberStore.getMemberNamespaces(userId, connection);
    } catch (SQLException e) {
      String message = "Error reading namespaces for user: " + userId;
      LOGGER.error(message, e);
      throw internalServerError(message);
    }
    LOGGER.debug("userId: {} namespaces: {}", userId, namespaces);

    if (ztsClient != null) {
      List<String> roles = ztsClient.getRoleAccess("tsdb.property", userId).getRoles();
      LOGGER.debug("userId: {} athens roles: {}", userId, roles);

      Set<String> tenantNamespaces = Utils.parseNamespaces(roles);
      LOGGER.debug("userId: {} namespaces: {}", userId, tenantNamespaces);

      if (!tenantNamespaces.isEmpty()) {
        List<Namespace> namespacesManagedInAthens = new ArrayList<>();
        for (String namespaceName : tenantNamespaces) {
          Namespace namespace;
          try {
            namespace = namespaceCache.getByName(namespaceName);
          } catch (Exception e) {
            String message = "Error reading namespace with name: " + namespaceName;
            LOGGER.error(message, e);
            throw internalServerError(message);
          }
          if (namespace == null) {
            LOGGER.error("Orphan athens role found for namespace: " + namespaceName);
          } else {

            // make a copy for not to mutate the state in the namespace cache.
            Namespace n = new Namespace();
            n.setId(namespace.getId());
            n.setName(namespace.getName());
            n.setAlias(namespace.getAlias());

            namespacesManagedInAthens.add(n);
          }
        }
        LOGGER.debug("userId: {} namespacesManagedInAthens: {}", userId, namespacesManagedInAthens);
        namespaces.addAll(namespacesManagedInAthens);
      }
    }

    return namespaces;
  }
}
