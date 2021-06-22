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

import net.opentsdb.horizon.NamespaceCache;
import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.model.Snooze;
import net.opentsdb.horizon.view.BatchContact;
import net.opentsdb.horizon.view.SnoozeView;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.EMPTY_MAP;
import static net.opentsdb.horizon.converter.AlertConverter.NOTIFICATION;
import static net.opentsdb.horizon.converter.AlertConverter.OBJECT_MAPPER;
import static net.opentsdb.horizon.converter.AlertConverter.RECIPIENTS;

public class SnoozeConverter extends BaseConverter<SnoozeView, Snooze> {

  private NamespaceCache namespaceCache;

  public static final String LABELS = "labels";
  public static final String ALERTIDS = "alertIds";
  public static final String FILTER = "filter";
  public static final String REASON = "reason";

  public SnoozeConverter(NamespaceCache namespaceCache) {
    this.namespaceCache = namespaceCache;
  }

  @Override
  public Snooze viewToModel(SnoozeView snoozeView) throws Exception {

    final Map<String, Object> notification = snoozeView.getNotification();
    BatchContact batchContact = null;
    Map<String, Object> definition = new HashMap<>();
    if (snoozeView.getNotification() != null) {
      batchContact = OBJECT_MAPPER.convertValue(notification.get(RECIPIENTS), BatchContact.class);
      definition.put(NOTIFICATION, notification);
    }

    putIfNotNull(definition, REASON, snoozeView.getReason());
    putIfNotNull(definition, LABELS, snoozeView.getLabels());
    putIfNotNull(definition, FILTER, snoozeView.getFilter());
    putIfNotNull(definition, ALERTIDS, snoozeView.getAlertIds());

    return Snooze.builder()
        .id(snoozeView.getId())
        .startTime(snoozeView.getStartTime())
        .endTime(snoozeView.getEndTime())
        .namespaceId(namespaceCache.getByName(snoozeView.getNamespace()).getId())
        .batchContact(batchContact)
        .definition(definition)
        .build();
  }

  private void putIfNotNull(
      final Map<String, Object> definition, final String key, final Object object) {
    if (Objects.nonNull(object)) {
      definition.put(key, object);
    }
  }

  @Override
  public SnoozeView modelToView(Snooze snooze) throws Exception {

    SnoozeView snoozeView = new SnoozeView();

    snoozeView.setId(snooze.getId());
    Namespace namespace = namespaceCache.getById(snooze.getNamespaceId());
    if (namespace != null) {
      snoozeView.setNamespace(namespace.getName());
      snoozeView.setNamespaceid(namespace.getId());
    }

    final Map<String, Object> definition = snooze.getDefinition();

    snoozeView.setAlertIds(
        (List<Integer>) Optional.ofNullable(definition.get(ALERTIDS)).orElse(EMPTY_LIST));
    snoozeView.setLabels(
        (List<String>) Optional.ofNullable(definition.get(LABELS)).orElse(EMPTY_LIST));
    snoozeView.setStartTime(snooze.getStartTime());
    snoozeView.setEndTime(snooze.getEndTime());
    snoozeView.setFilter(
        (Map<String, Object>) Optional.ofNullable(definition.get(FILTER)).orElse(EMPTY_MAP));
    snoozeView.setNotification(
        (Map<String, Object>) Optional.ofNullable(definition.get(NOTIFICATION)).orElse(EMPTY_MAP));

    if (definition.containsKey(REASON)) {
      snoozeView.setReason(String.valueOf(definition.get(REASON)));
    } else {
      snoozeView.setReason(null);
    }
    snoozeView.setCreatedBy(snooze.getCreatedBy());
    snoozeView.setCreatedTime(snooze.getCreatedTime());
    snoozeView.setUpdatedBy(snooze.getUpdatedBy());
    snoozeView.setUpdatedTime(snooze.getUpdatedTime());

    return snoozeView;
  }
}
