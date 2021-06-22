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
import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.model.User;
import net.opentsdb.horizon.store.NamespaceFollowerStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.BadRequestException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static net.opentsdb.horizon.profile.Utils.validateNamespace;
import static net.opentsdb.horizon.service.BaseService.internalServerError;
import static net.opentsdb.horizon.util.Utils.isNullOrEmpty;

public class NamespaceFollowerService {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(net.opentsdb.horizon.service.NamespaceMemberService.class);

  private final NamespaceFollowerStore store;

  private final AuthService authService;
  private NamespaceCache namespaceCache;

  public NamespaceFollowerService(
      final NamespaceFollowerStore store,
      final AuthService authService,
      final NamespaceCache namespaceCache) {
    this.store = store;
    this.authService = authService;
    this.namespaceCache = namespaceCache;
  }

  public List<User> getNamespaceFollowers(final int namespaceId) {
    try (final Connection connection = store.getReadOnlyConnection()) {
      return store.getNamespaceFollowers(namespaceId, connection);
    } catch (SQLException sqlException) {
      String message = "Error reading followers for namespace id: " + namespaceId;
      LOGGER.error(message, sqlException);
      throw internalServerError(message);
    }
  }

  public void addNamespaceFollower(final int namespaceId, String principal) {

    // check user/service is already member of namespace
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
      if (authService.authorize(namespace, principal)) {
        throw new BadRequestException("Namespace member cannot be a follower");
      }
    } catch (SQLException e) {
      String message = "Error doing the authz check";
      LOGGER.error(message, e);
      throw internalServerError(message);
    }

    try (Connection connection = store.getReadWriteConnection()) {
      try {
        store.addFollowers(namespaceId, Arrays.asList(principal), connection);
        store.commit(connection);
      } catch (SQLException e) {
        store.rollback(connection);
        throw e;
      }
    } catch (SQLException sqlException) {
      String message = "Error adding follower to namespace: " + namespace.getName();
      LOGGER.error(message, sqlException);
      throw internalServerError(message);
    }
  }

  public void removeNamespaceFollower(final int namespaceId, String principal) {
    try (Connection connection = store.getReadWriteConnection()) {
      try {
        store.removeFollowers(
            namespaceId, Arrays.asList(principal), connection);
        store.commit(connection);
      } catch (SQLException e) {
        store.rollback(connection);
        throw e;
      }
    } catch (SQLException sqlException) {
      String message = "Error removing follower from namespace id: " + namespaceId;
      LOGGER.error(message, sqlException);
      throw internalServerError(message);
    }
  }

  public List<Namespace> myFollowingNamespaces(String userId, HttpServletRequest request) {
    if (isNullOrEmpty(userId)) {
      userId = request.getUserPrincipal().getName();
    }
    return getFollowingNamespaces(userId);
  }

  private List<Namespace> getFollowingNamespaces(String userId) {
    try (Connection con = store.getReadOnlyConnection()) {
      return store.getFollowingNamespaces(userId, con);
    } catch (SQLException e) {
      String message = "Error reading following namespace for user " + userId;
      LOGGER.error(message, e);
      throw internalServerError(message);
    }
  }
}
