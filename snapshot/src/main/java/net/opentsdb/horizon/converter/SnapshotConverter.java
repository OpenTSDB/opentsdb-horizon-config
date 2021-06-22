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

package net.opentsdb.horizon.converter;

import net.opentsdb.horizon.model.Snapshot;
import net.opentsdb.horizon.util.Utils;
import net.opentsdb.horizon.view.SnapshotView;
import net.opentsdb.horizon.view.SourceType;

public class SnapshotConverter extends BaseConverter<SnapshotView, Snapshot> {

  @Override
  public Snapshot viewToModel(SnapshotView view) {
    Snapshot model = new Snapshot();
    model.setId(view.getId());
    model.setName(view.getName());
    SourceType sourceType = view.getSourceType();
    if (sourceType != null) {
      model.setSourceType(sourceType.id);
    } else {
      model.setSourceType(NOT_PASSED);
    }
    model.setSourceId(view.getSourceId());
    model.setContent(view.getContent());
    model.setCreatedBy(view.getCreatedBy());
    model.setCreatedTime(view.getCreatedTime());
    model.setUpdatedBy(view.getUpdatedBy());
    model.setUpdatedTime(view.getUpdatedTime());
    return model;
  }

  @Override
  public SnapshotView modelToView(Snapshot model) {
    SnapshotView view = new SnapshotView();
    view.setId(model.getId());
    view.setName(model.getName());
    byte sourceType = model.getSourceType();
    if (sourceType > 0) {
      view.setSourceType(SourceType.valueOf(sourceType));
    }
    view.setSourceId(model.getSourceId());
    view.setCreatedBy(model.getCreatedBy());
    view.setCreatedTime(model.getCreatedTime());
    view.setUpdatedBy(model.getUpdatedBy());
    view.setUpdatedTime(model.getUpdatedTime());
    view.setLastVisitedTime(model.getLastVisitedTime());
    view.setSourceName(model.getSourceName());
    String slug = Utils.slugify(model.getName());
    view.setPath("/" + model.getId() + "/" + slug);
    return view;
  }
}
