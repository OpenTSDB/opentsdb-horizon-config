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

import net.opentsdb.horizon.fs.Path;
import net.opentsdb.horizon.fs.model.Folder;
import net.opentsdb.horizon.fs.view.FolderType;
import net.opentsdb.horizon.converter.BaseConverter;
import net.opentsdb.horizon.store.BaseStore;

import java.sql.Timestamp;
import java.util.List;

public abstract class ProfileService<View, Model, Converter extends BaseConverter<View, Model>>
    extends BaseService<View, Model, Converter> {

  public ProfileService(Converter converter, BaseStore store) {
    super(converter, store);
  }

  protected void createHomeFolder(
      Path homePath, String createdBy, Timestamp createTime, List<Folder> folders) {

    Folder homeFolder = new Folder();
    homeFolder.setName("Home");
    homeFolder.setType(FolderType.DASHBOARD);
    homeFolder.setPath(homePath.getPath());
    homeFolder.setPathHash(homePath.hash());
    homeFolder.setCreatedBy(createdBy);
    homeFolder.setUpdatedBy(createdBy);
    homeFolder.setCreatedTime(createTime);
    homeFolder.setUpdatedTime(createTime);

    Folder trashFolder = new Folder();
    trashFolder.setName("Trash");
    trashFolder.setType(FolderType.DASHBOARD);
    trashFolder.setPath(homePath.getChildPath(trashFolder.getName()));
    trashFolder.setPathHash(homePath.hash(trashFolder.getPath()));
    trashFolder.setParentPathHash(homeFolder.getPathHash());
    trashFolder.setCreatedBy(createdBy);
    trashFolder.setUpdatedBy(createdBy);
    trashFolder.setCreatedTime(createTime);
    trashFolder.setUpdatedTime(createTime);

    folders.add(homeFolder);
    folders.add(trashFolder);
  }
}
