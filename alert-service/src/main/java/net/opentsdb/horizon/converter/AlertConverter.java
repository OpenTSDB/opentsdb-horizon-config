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

import com.fasterxml.jackson.databind.ObjectMapper;
import net.opentsdb.horizon.NamespaceCache;
import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.model.Alert;
import net.opentsdb.horizon.model.Contact;
import net.opentsdb.horizon.util.Utils;
import net.opentsdb.horizon.view.AlertView;
import net.opentsdb.horizon.view.BatchContact;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.opentsdb.horizon.util.Utils.isNullOrEmpty;
import static net.opentsdb.horizon.util.Utils.slugify;

public class AlertConverter extends BaseConverter<AlertView, Alert> {

  public static final String RECIPIENTS = "recipients";
  public static final String QUERIES = "queries";
  public static final String THRESHOLD = "threshold";
  public static final String NOTIFICATION = "notification";
  public static final String GROUPING_RULES = "GROUPING_RULES";
  public static final String VERSION = "version";
  public static final String CREATED_FROM = "createdFrom";

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final NamespaceCache namespaceCache;

  private final BatchContactConverter contactConverter;

  public AlertConverter(NamespaceCache namespaceCache) {
    this.namespaceCache = namespaceCache;
    this.contactConverter = new BatchContactConverter();
  }

  @Override
  public Alert viewToModel(AlertView alertView) {
    Alert alert = new Alert();
    alert.setId(alertView.getId());
    alert.setNamespaceId(alertView.getNamespaceId());
    alert.setName(alertView.getName());
    if (alertView.getEnabled() != null) {
      alert.setEnabled(alertView.getEnabled());
    }
    alert.setType(alertView.getType());
    alert.setLabels(alertView.getLabels());

    if (alertView.getNotification() != null) {
      Map<String, Object> notification = alertView.getNotification();
      BatchContact batchContact = OBJECT_MAPPER.convertValue(notification.get(RECIPIENTS), BatchContact.class);
      alert.setContacts(batchContact);
      notification.remove(RECIPIENTS);
    }

    Map<String, Object> queries = alertView.getQueries();
    Map<String, Object> threshold = alertView.getThreshold();
    Map<String, Object> notification = alertView.getNotification();
    List<String> alertGroupingRules = alertView.getAlertGroupingRules();
    Map<String, Object> createdFrom = alertView.getCreatedFrom();

    Map<String, Object> definition = new HashMap<>();

    if (!isNullOrEmpty(queries)) {
      definition.put(QUERIES, queries);
    }
    if (!isNullOrEmpty(threshold)) {
      definition.put(THRESHOLD, threshold);
    }
    if (!isNullOrEmpty(notification)) {
      definition.put(NOTIFICATION, notification);
    }
    if (null != alertGroupingRules) {
      definition.put(GROUPING_RULES, alertGroupingRules);
    }
    if (!isNullOrEmpty(createdFrom)) {
      definition.put(CREATED_FROM, createdFrom);
    }

    alert.setVersion(alertView.getVersion());

    if (!definition.isEmpty()) {
      alert.setDefinition(definition);
    }

    alert.setCreatedBy(alertView.getCreatedBy());
    alert.setCreatedTime(alertView.getCreatedTime());
    alert.setUpdatedBy(alertView.getUpdatedBy());
    alert.setUpdatedTime(alertView.getUpdatedTime());
    return alert;
  }

  @Override
  public AlertView modelToView(Alert alert) throws Exception{
    AlertView alertView = new AlertView();
    alertView.setId(alert.getId());
    alertView.setName(alert.getName());
    alertView.setSlug(slugify(alert.getName()));
    alertView.setEnabled(alert.isEnabled());
    alertView.setDeleted(alert.isDeleted());
    alertView.setType(alert.getType());
    alertView.setLabels(alert.getLabels());

    final BatchContact batchContact;
    List<Contact> contactList = alert.getContactList();
    if (Utils.isNullOrEmpty(contactList)) {
      batchContact = alert.getContacts();
    } else {
      batchContact = contactConverter.modelToView(contactList);
    }

    Map<String, Object> definition = alert.getDefinition();
    if (definition != null) {
      Map<String, Object> queries = (Map<String, Object>) definition.get(QUERIES);
      Map<String, Object> threshold = (Map<String, Object>) definition.get(THRESHOLD);
      Map<String, Object> notification = (Map<String, Object>) definition.get(NOTIFICATION);
      List<String> grouping_rules = (List<String>) definition.get(GROUPING_RULES);
      Map<String, Object> createdFrom = (Map<String, Object>) definition.get(CREATED_FROM);

      notification.put(RECIPIENTS, batchContact);

      alertView.setQueries(queries);
      alertView.setThreshold(threshold);
      alertView.setNotification(notification);
      alertView.setAlertGroupingRules(grouping_rules);
      alertView.setCreatedFrom(createdFrom);
    } else {
      alertView.setRecipients(batchContact);
    }
    Namespace namespace = namespaceCache.getById(alert.getNamespaceId());
    if (namespace != null) {
      alertView.setNamespace(namespace.getName());
      alertView.setNamespaceId(namespace.getId());
    }
    alertView.setVersion(alert.getVersion());
    alertView.setCreatedBy(alert.getCreatedBy());
    alertView.setCreatedTime(alert.getCreatedTime());
    alertView.setUpdatedBy(alert.getUpdatedBy());
    alertView.setUpdatedTime(alert.getUpdatedTime());
    return alertView;
  }
}
