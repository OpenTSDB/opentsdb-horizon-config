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

import net.opentsdb.horizon.converter.UserConverter;
import net.opentsdb.horizon.fs.Path;
import net.opentsdb.horizon.fs.Path.PathException;
import net.opentsdb.horizon.fs.model.Folder;
import net.opentsdb.horizon.fs.store.FolderStore;
import net.opentsdb.horizon.model.User;
import net.opentsdb.horizon.model.User.CreationMode;
import net.opentsdb.horizon.store.UserStore;
import net.opentsdb.horizon.store.StoreFunction;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class UserService extends ProfileService<User, User, UserConverter> {

  private final UserStore store;
  private FolderStore folderStore;

  public UserService(final UserStore store, final FolderStore folderStore) {
    super(new UserConverter(), store);
    this.store = store;
    this.folderStore = folderStore;
  }

  public User create(final String userId) {
    User user = new User();
    user.setUserid(userId);
    user.setName("");

    create(user, userId);
    return user;
  }

  public User provision(final String userId) {
    logger.debug("Provisioning user: {}", userId);
    String format = "Error provisioning user: %s";
    User user = get((connection) -> store.getById(userId, connection), format, userId);
    if (user == null) {
      user = create(userId);
    }
    return user;
  }

  @Override
  protected void doCreate(User user, Connection connection) throws PathException, SQLException {
    store.create(user, connection);

    List<Folder> folders = new ArrayList<>();
    Path path = Path.getByUserId(user.getUserid());
    createHomeFolder(path, user.getUserid(), user.getUpdatedtime(), folders);
    folderStore.createFolder(folders, connection);
  }

  @Override
  protected void doCreates(List<User> users, Connection connection, String principal)
      throws PathException, SQLException {
    store.create(users, connection);
    List<Folder> folders = new ArrayList<>();
    for (User user : users) {
      Path path = Path.getByUserId(user.getUserid());
      createHomeFolder(path, user.getUserid(), user.getUpdatedtime(), folders);
    }
    folderStore.createFolder(folders, connection);
  }

  @Override
  protected void setCreatorUpdatorIdAndTime(User user, String ignored, Timestamp timestamp) {
    user.setUpdatedtime(timestamp);
  }

  public User getById(String id) {
    String format = "Error reading user by id: %s";
    User user = get((connection) -> store.getById(id, connection), format, id);
    if (user == null) {
      throw notFoundException("User not found id: " + id);
    }
    return user;
  }

  public List<User> getAll(boolean includeDisabled) {
    StoreFunction<Connection, List<User>> function;
    if (includeDisabled) {
      function = (connection) -> store.getAll(connection);
    } else {
      function = (connection) -> store.getUsers(connection, true);
    }
    final String message = "Error listing all users";
    return list(function, message);
  }

  public void createOrUpdate(List<User> users, String principal) {
    Timestamp now = now();
    users.stream()
        .forEach(
            user -> {
              preCreate(user);
              setCreatorUpdatorIdAndTime(user, principal, now);
            });
    try (Connection con = store.getReadWriteConnection()) {
      try {
        int[] counts = store.createOrUpdate(users, con);
        List<User> newUsers = getNewUsers(users, counts);

        List<Folder> folders = new ArrayList();
        for (User user : newUsers) {
          Path path = Path.getByUserId(user.getUserid());
          createHomeFolder(path, user.getUserid(), user.getUpdatedtime(), folders);
        }
        if (!folders.isEmpty()) {
          folderStore.createFolder(folders, con);
        }

        store.commit(con);
      } catch (Exception e) {
        store.rollback(con);
        throw e;
      }
    } catch (Exception e) {
      String message = "Error persisting user";
      logger.error(message, e);
      throw internalServerError(message);
    }
  }

  private List<User> getNewUsers(List<User> users, int[] counts) {
    List<User> newUsers = new ArrayList<>();
    for (int i = 0; i < counts.length; i++) {
      if (counts[i] == 1) {
        newUsers.add(users.get(i));
      }
    }
    return newUsers;
  }

  @Override
  protected void preCreate(User user) {
    if (user.isEnabled() == null) {
      user.setEnabled(true);
    }
    if (user.getCreationmode() == null) {
      user.setCreationmode(CreationMode.onthefly);
    }
  }
}
