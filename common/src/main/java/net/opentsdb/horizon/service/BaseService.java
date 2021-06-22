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

import net.opentsdb.horizon.converter.BaseConverter;
import net.opentsdb.horizon.store.BaseStore;
import net.opentsdb.horizon.store.StoreFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Typically this class's methods should not be called directly in production. Best way is to use
 * AuthencatedBaseService class and call its create methods.
 *
 * @param <View>
 * @param <Model>
 */
public abstract class BaseService<View, Model, Converter extends BaseConverter<View, Model>> {

  protected Logger logger = LoggerFactory.getLogger(getClass());

  protected Converter converter;

  protected BaseStore store;

  public BaseService(Converter converter, BaseStore store) {
    this.converter = converter;
    this.store = store;
  }

  public View create(View view, String principal) {
    Model model;
    try {
      model = converter.viewToModel(view);
    } catch (Exception e) {
      throw internalServerError(e.getMessage());
    }
    preCreate(model);
    setCreatorUpdatorIdAndTime(model, principal, now());

    try (Connection con = store.getReadWriteConnection()) {
      try {
        doCreate(model, con);
        store.commit(con);
      } catch (Exception e) {
        String type = model.getClass().getSimpleName();
        logger.error("Error creating " + type, e);
        store.rollback(con);
        throw e;
      }
    } catch (WebApplicationException e) {
      throw e;
    } catch (SQLException e) {
      handleSqlError(e);
    } catch (Exception e) {
      throw internalServerError(e.getMessage());
    }

    try {
      return toView(model);
    } catch (Exception e) {
      throw internalServerError(e.getMessage());
    }
  }

  public List<View> creates(List<View> views, String principal) {
    Timestamp now = now();

    Model model = null;

    List<Model> models = new ArrayList<>(views.size());
    for (View view : views) {
      try {
        model = toModel(view);
      } catch (Exception e) {
        throw internalServerError(e.getMessage());
      }
      preCreate(model);
      setCreatorUpdatorIdAndTime(model, principal, now);
      models.add(model);
    }

    try (Connection con = store.getReadWriteConnection()) {
      try {
        doCreates(models, con, principal);
        store.commit(con);
      } catch (Exception e) {
        String type = model.getClass().getSimpleName();
        logger.error("Error creating " + type, e);
        store.rollback(con);
        throw e;
      }
    } catch (WebApplicationException e) {
      throw e;
    } catch (SQLException e) {
      handleSqlError(e);
    } catch (Exception e) {
      throw internalServerError(e.getMessage());
    }

    return toViews(models);
  }

  public View update(View view, String principal) {
    Model model;
    try {
      model = converter.viewToModel(view);
    } catch (Exception e) {
      throw internalServerError(e.getMessage());
    }

    preUpdate(model);
    setUpdaterIdAndTime(model, principal, now());

    try (Connection connection = store.getReadWriteConnection()) {
      try {
        model = doUpdate(model, connection);
        store.commit(connection);
      } catch (Exception e) {
        String type = model.getClass().getSimpleName();
        logger.error("Error updating " + type, e);
        store.rollback(connection);
        throw e;
      }
    } catch (WebApplicationException e) {
      throw e;
    } catch (SQLException e) {
      handleSqlError(e);
    } catch (Exception e) {
      throw internalServerError(e.getMessage());
    }

    try {
      return toView(model);
    } catch (Exception e) {
      throw internalServerError(e.getMessage());
    }
  }

  public List<View> updates(List<View> views, String principal) {
    Timestamp now = now();
    Model model = null;
    List<Model> models = new ArrayList<>();
    for (View view : views) {
      try {
        model = toModel(view);
      } catch (Exception e) {
        throw internalServerError(e.getMessage());
      }
      preUpdate(model);
      setUpdaterIdAndTime(model, principal, now);
      models.add(model);
    }
    try (Connection con = store.getReadWriteConnection()) {
      try {
        doUpdates(models, con);
        store.commit(con);
      } catch (Exception e) {
        String type = model.getClass().getSimpleName();
        logger.error("Error updating " + type, e);
        store.rollback(con);
        throw e;
      }
    } catch (WebApplicationException e) {
      throw e;
    } catch (SQLException e) {
      handleSqlError(e);
    } catch (Exception e) {
      throw internalServerError(e.getMessage());
    }
    return toViews(models);
  }

  public void delete(View view) {
    Model model;
    try {
      model = converter.viewToModel(view);
    } catch (Exception e) {
      throw internalServerError(e.getMessage());
    }
    try (Connection con = store.getReadWriteConnection()) {
      try {
        doDelete(model, con);
        store.commit(con);
      } catch (Exception e) {
        String type = model.getClass().getSimpleName();
        logger.error("Error deleting " + type, e);
        store.rollback(con);
        throw e;
      }
    } catch (WebApplicationException e) {
      throw e;
    } catch (SQLException e) {
      handleSqlError(e);
    } catch (Exception e) {
      throw internalServerError(e.getMessage());
    }
  }

  protected View get(
      StoreFunction<Connection, Model> function, String messageFormat, Object... args) {
    try (Connection connection = store.getReadOnlyConnection()) {
      return toView(function.apply(connection));
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      String message = String.format(messageFormat, args);
      logger.error(message, e);
      throw internalServerError(message);
    }
  }

  protected List<View> list(
      StoreFunction<Connection, List<Model>> function, String messageFormat, Object... args) {
    try (Connection connection = store.getReadOnlyConnection()) {
      return toViews(function.apply(connection));
    } catch (SQLException | IOException e) {
      String message = String.format(messageFormat, args);
      logger.error(message, e);
      throw internalServerError(message);
    }
  }

  protected void preCreate(Model model) {}

  protected void preUpdate(Model model) {}

  protected void setCreatorUpdatorIdAndTime(Model model, String principal, Timestamp timestamp) {
    setCreatorIdAndTime(model, principal, timestamp);
    setUpdaterIdAndTime(model, principal, timestamp);
  }

  protected void setCreatorIdAndTime(Model model, String principal, Timestamp timestamp) {
    throw new UnsupportedOperationException();
  }

  protected void setUpdaterIdAndTime(Model model, String principal, Timestamp timestamp) {
    throw new UnsupportedOperationException();
  }

  protected void doCreate(Model model, Connection connection) throws Exception {
    throw new UnsupportedOperationException();
  }

  protected void doCreates(List<Model> models, Connection connection, String principal)
      throws Exception {
    throw new UnsupportedOperationException();
  }

  protected void doUpdates(List<Model> models, Connection connection) throws Exception {
    throw new UnsupportedOperationException();
  }

  protected Model doUpdate(Model model, Connection connection) throws Exception {
    throw new UnsupportedOperationException();
  }

  protected void doDelete(Model model, Connection connection) throws Exception {
    throw new UnsupportedOperationException();
  }

  protected View toView(Model model) throws Exception {
    View view = null;
    if (null != model) {
      view = converter.modelToView(model);
    }
    return view;
  }

  protected Model toModel(View view) throws Exception {
    Model model = null;
    if (null != view) {
      model = converter.viewToModel(view);
    }
    return model;
  }

  protected List<View> toViews(List<Model> models) {
    return models.stream()
        .filter(model -> null != model)
        .map(
            model -> {
              try {
                return toView(model);
              } catch (Exception e) {
                throw internalServerError(e.getMessage());
              }
            })
        .collect(Collectors.toList());
  }

  public static Timestamp now() {
    return new Timestamp(System.currentTimeMillis());
  }

  public static ForbiddenException forbiddenException(String message) {
    Response response = Response.status(Response.Status.FORBIDDEN).entity(message).build();
    return new ForbiddenException(response);
  }

  public static BadRequestException badRequestException(String message) {
    Response response = Response.status(Response.Status.BAD_REQUEST).entity(message).build();
    return new BadRequestException(response);
  }

  public static NotFoundException notFoundException(String message) {
    Response response = Response.status(Response.Status.NOT_FOUND).entity(message).build();
    return new NotFoundException(response);
  }

  public static ClientErrorException conflictException(String message) {
    Response response = Response.status(Response.Status.CONFLICT).entity(message).build();
    return new ClientErrorException(response);
  }

  public static InternalServerErrorException internalServerError(String message) {
    Response response =
        Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(message).build();
    return new InternalServerErrorException(response);
  }

  private void handleSqlError(SQLException e) {
    String message = store.formatErrorMessage(e);
    if (e.getErrorCode() == 1062) {
      throw conflictException(message);
    } else {
      throw internalServerError(message);
    }
  }
}
