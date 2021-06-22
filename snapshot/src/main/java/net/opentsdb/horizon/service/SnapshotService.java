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

import net.opentsdb.horizon.fs.model.File;
import net.opentsdb.horizon.fs.store.FolderStore;
import net.opentsdb.horizon.fs.view.FolderType;
import net.opentsdb.horizon.model.Alert;
import net.opentsdb.horizon.model.ContentHistory;
import net.opentsdb.horizon.model.Snapshot;
import net.opentsdb.horizon.store.AlertStore;
import net.opentsdb.horizon.converter.ContentConverter;
import net.opentsdb.horizon.converter.SnapshotConverter;
import net.opentsdb.horizon.model.Content;
import net.opentsdb.horizon.store.SnapshotStore;
import net.opentsdb.horizon.view.SnapshotView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static net.opentsdb.horizon.converter.BaseConverter.NOT_PASSED;
import static net.opentsdb.horizon.util.Utils.isNullOrEmpty;
import static net.opentsdb.horizon.view.SourceType.ALERT;
import static net.opentsdb.horizon.view.SourceType.DASHBOARD;
import static net.opentsdb.horizon.view.SourceType.SNAPSHOT;

public class SnapshotService extends CommonService<SnapshotView, Snapshot, SnapshotConverter> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotService.class);

  private static final byte NO_SOURCE = 0;

  private SnapshotStore store;
  private ContentService contentService;
  private FolderStore folderStore;
  private AlertStore alertStore;
  private ActivityJobScheduler activityJobScheduler;

  public SnapshotService(
      SnapshotStore store,
      ContentService contentService,
      FolderStore folderStore,
      AlertStore alertStore,
      ActivityJobScheduler activityJobScheduler) {
    super(new SnapshotConverter(), store);
    this.store = store;
    this.contentService = contentService;
    this.folderStore = folderStore;
    this.alertStore = alertStore;
    this.activityJobScheduler = activityJobScheduler;
  }

  @Override
  protected void preCreate(Snapshot model) {
    if (model.getSourceType() == NOT_PASSED) {
      model.setSourceType(NO_SOURCE);
      model.setSourceId(NO_SOURCE);
    }
  }

  @Override
  protected void doCreate(Snapshot snapshot, Connection connection)
      throws Exception {

    String name = snapshot.getName();
    if (!isNullOrEmpty(name)) {
      snapshot.setName(name.trim());
    }
    if (isNullOrEmpty(snapshot.getName())) {
      throw badRequestException("Snapshot name empty");
    }

    if (!isSourceFound(connection, snapshot.getSourceType(), snapshot.getSourceId())) {
      throw badRequestException("Source not found");
    }

    Object contentView = snapshot.getContent();
    if (contentView == null) {
      throw badRequestException("Content not found");
    }

    Content content = contentService.viewToModel(contentView);
    content.setCreatedBy(snapshot.getCreatedBy());
    content.setCreatedTime(snapshot.getCreatedTime());

    snapshot.setContentId(content.getSha2());

    contentService.createContent(connection, content);
    store.create(connection, snapshot);

    ContentHistory history = new ContentHistory();
    history.setContentType(SNAPSHOT.id);
    history.setEntityId(snapshot.getId());
    history.setContentId(snapshot.getContentId());
    history.setCreatedBy(snapshot.getCreatedBy());
    history.setCreatedTime(snapshot.getCreatedTime());

    contentService.createContentHistory(connection, history);
  }

  @Override
  protected Snapshot doUpdate(Snapshot snapshot, Connection connection)
      throws SQLException, IOException {
    long id = snapshot.getId();
    Snapshot fromDB = store.getById(connection, id);

    if (null == fromDB) {
      throw notFoundException("Snapshot not found with id: " + id);
    }

    if (!fromDB.getCreatedBy().equals(snapshot.getUpdatedBy())) {
      throw forbiddenException("Access denied");
    }

    String newName = snapshot.getName();
    if (newName != null) {
      newName = newName.trim();
      if (isNullOrEmpty(newName)) {
        throw badRequestException("Snapshot name empty");
      }
      fromDB.setName(newName);
    }

    boolean sourceChanged = false;
    if (snapshot.getSourceType() > NOT_PASSED) {
      sourceChanged = fromDB.getSourceType() != snapshot.getSourceType();
      fromDB.setSourceType(snapshot.getSourceType());
    }

    if (snapshot.getSourceId() > NOT_PASSED) {
      sourceChanged = fromDB.getSourceId() != snapshot.getSourceId();
      fromDB.setSourceId(snapshot.getSourceId());
    }

    if (sourceChanged && !isSourceFound(connection, fromDB.getSourceType(), fromDB.getSourceId())) {
      throw badRequestException("Source not found");
    }
    if (fromDB.getSourceType() == SNAPSHOT.id && fromDB.getSourceId() == fromDB.getId()) {
      throw badRequestException("Source pointing to the snapshot itself");
    }

    Object contentView = snapshot.getContent();
    if (contentView != null) {
      String updatedBy = snapshot.getUpdatedBy();
      Timestamp updatedTime = snapshot.getUpdatedTime();

      Content content = contentService.viewToModel(contentView);

      if (!Arrays.equals(fromDB.getContentId(), content.getSha2())) {
        content.setCreatedBy(updatedBy);
        content.setCreatedTime(updatedTime);

        fromDB.setContentId(content.getSha2());
        contentService.createContent(connection, content);

        ContentHistory history = new ContentHistory();
        history.setContentType(SNAPSHOT.id);
        history.setEntityId(snapshot.getId());
        history.setContentId(content.getSha2());
        history.setCreatedBy(updatedBy);
        history.setCreatedTime(updatedTime);

        contentService.createContentHistory(connection, history);
      }
    }
    fromDB.setUpdatedBy(snapshot.getUpdatedBy());
    fromDB.setUpdatedTime(snapshot.getUpdatedTime());

    store.update(connection, fromDB);
    return fromDB;
  }

  public SnapshotView getById(final long id, final String userId) {
    final AtomicReferenceArray<byte[]> content = new AtomicReferenceArray(1);
    SnapshotView view =
        get(
            connection -> {
              Snapshot model = store.getSnapshotAndContentById(id, connection);
              if (null != model) {
                model.setSourceName(
                    getSourceName(connection, model.getSourceType(), model.getSourceId()));
                content.set(0, (byte[]) model.getContent());
                activityJobScheduler.addActivity(userId, SNAPSHOT.id, id);
              }
              return model;
            },
            "Error reading snapshot by id: %d",
            id);
    if (view == null) {
      throw notFoundException("Snapshot not found with id: " + id);
    }
    try {
      view.setContent(ContentConverter.modelToView(content.get(0)));
    } catch (IOException exception) {
      throw internalServerError("Error deserializing the content of snapshot by id : " + id);
    }
    return view;
  }

  private boolean isSourceFound(Connection connection, byte sourceType, long sourceId)
      throws SQLException, IOException {
    if (sourceType == DASHBOARD.id) {
      File fileById = folderStore.getFileById(FolderType.DASHBOARD, sourceId, connection);
      return fileById != null;
    } else if (sourceType == ALERT.id) {
      Alert alert = alertStore.get(sourceId, false, false, connection);
      return alert != null;
    } else if (sourceType == SNAPSHOT.id) {
      Snapshot snapshot = store.getById(connection, sourceId);
      return snapshot != null;
    } else {
      return true;
    }
  }

  private String getSourceName(Connection connection, byte sourceType, long sourceId)
      throws SQLException, IOException {
    String sourceName = null;
    if (sourceType == DASHBOARD.id) {
      File fileById = folderStore.getFileById(FolderType.DASHBOARD, sourceId, connection);
      if (fileById != null) {
        sourceName = fileById.getName();
      }
    } else if (sourceType == ALERT.id) {
      Alert alert = alertStore.get(sourceId, false, false, connection);
      if (alert != null) {
        sourceName = alert.getName();
      }
    } else if (sourceType == SNAPSHOT.id) {
      Snapshot snapshot = store.getById(connection, sourceId);
      if (snapshot != null) {
        sourceName = snapshot.getName();
      }
    } else {
      sourceName = null;
    }
    return sourceName;
  }

  public List<SnapshotView> getRecentlyVisited(final String userId, final int limit) {
    final String format = "Error listing recently visited snapshots for user: %s";
    List<SnapshotView> snapshots =
        list(
            (connection) -> {
              try {
                return store.getRecentlyVisited(connection, userId, limit);
              } catch (SQLException exception) {
                String message = "Error reading recently visited snapshots for user: " + userId;
                LOGGER.error(message, exception);
                throw internalServerError(message);
              }
            },
            format,
            userId);
    return snapshots;
  }
}
