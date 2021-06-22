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
import net.opentsdb.horizon.converter.BaseConverter;
import net.opentsdb.horizon.store.BaseStore;

import javax.ws.rs.ForbiddenException;
import java.sql.SQLException;
import java.util.List;

public abstract class AuthenticatedBaseService<
        View, Model, Converter extends BaseConverter<View, Model>>
    extends BaseService<View, Model, Converter> {

  protected final NamespaceCache namespaceCache;
  private final AuthService authService;

  public AuthenticatedBaseService(
      Converter converter,
      BaseStore store,
      AuthService authService,
      final NamespaceCache namespaceCache) {
    super(converter, store);
    this.authService = authService;
    this.namespaceCache = namespaceCache;
  }

  public View create(View view, Namespace namespace, String principal) {
    authorize(namespace, principal);

    return create(view, principal);
  }

  public List<View> creates(List<View> views, Namespace namespace, String principal) {
    authorize(namespace, principal);

    return creates(views, principal);
  }

  public View update(View view, Namespace namespace, String principal) {
    authorize(namespace, principal);

    return update(view, principal);
  }

  public List<View> updates(List<View> views, Namespace namespace, String principal) {
    authorize(namespace, principal);

    return updates(views, principal);
  }

  public void delete(View view, Namespace namespace, String principal) {
    authorize(namespace, principal);

    delete(view);
  }

  protected void authorize(Namespace namespace, String principal) {
    try {
      if (!authService.authorize(namespace, principal)) {
        throw new ForbiddenException(
            "Only namespace members are allowed to create/update/delete alerts.");
      }
    } catch (SQLException e) {
      String message = "Error doing the authz check";
      logger.error(message, e);
      throw internalServerError(message);
    }
  }
}
