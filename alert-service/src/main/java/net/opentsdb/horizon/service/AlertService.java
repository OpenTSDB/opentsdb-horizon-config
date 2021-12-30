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
import net.opentsdb.horizon.converter.AlertConverter;
import net.opentsdb.horizon.model.Alert;
import net.opentsdb.horizon.model.Contact;
import net.opentsdb.horizon.model.ContactType;
import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.store.AlertStore;
import net.opentsdb.horizon.store.ContactStore;
import net.opentsdb.horizon.view.AlertView;
import net.opentsdb.horizon.view.BatchContact;
import net.opentsdb.horizon.view.EmailContact;
import net.opentsdb.horizon.view.HttpContact;
import net.opentsdb.horizon.view.OCContact;
import net.opentsdb.horizon.view.OpsGenieContact;
import net.opentsdb.horizon.view.PagerDutyContact;
import net.opentsdb.horizon.view.SlackContact;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static net.opentsdb.horizon.converter.AlertConverter.CREATED_FROM;
import static net.opentsdb.horizon.converter.AlertConverter.GROUPING_RULES;
import static net.opentsdb.horizon.converter.AlertConverter.NOTIFICATION;
import static net.opentsdb.horizon.converter.AlertConverter.QUERIES;
import static net.opentsdb.horizon.converter.AlertConverter.THRESHOLD;
import static net.opentsdb.horizon.converter.AlertConverter.VERSION;
import static net.opentsdb.horizon.profile.Utils.validateNamespace;
import static net.opentsdb.horizon.converter.BaseConverter.NOT_PASSED;
import static net.opentsdb.horizon.util.Utils.isNullOrEmpty;

public class AlertService extends AuthenticatedBaseService<AlertView, Alert, AlertConverter> {

  public static final Integer DEFAULT_VERSION = 0;

  public static String SO_SERVICE = "HZ_ALERT_SERVICE";

  private ContactStore contactStore;
  private AlertStore store;

  public AlertService(
      final AlertStore alertStore,
      final AuthService authService,
      final NamespaceCache namespaceCache,
      final ContactStore contactStore) {

    super(new AlertConverter(namespaceCache), alertStore, authService, namespaceCache);
    this.store = alertStore;
    this.contactStore = contactStore;
  }

  @Override
  protected void doCreates(List<Alert> alerts, Connection connection, String principal)
      throws IOException, SQLException {
    if (!alerts.isEmpty()) {
      store.create(alerts, connection);
      for (Alert alert : alerts) {
        addToAlertContact(alert, connection);
      }
    }
  }

  private void addToAlertContact(Alert alert, Connection connection)
      throws IOException, SQLException {

    BatchContact batchContact = alert.getContacts();
    if (batchContact == null) {
      return;
    }

    List<Integer> incomingContactIds = new ArrayList<>();

    List<EmailContact> incomingEmailContacts = batchContact.getEmail();
    if (incomingEmailContacts != null) {
      // Admin email contacts are added on the fly. The following block checks for such admin email
      // contact and add it to the contact table.
      String userName = alert.getUpdatedBy();
      Timestamp createdTime = alert.getUpdatedTime();
      List<EmailContact> newEmails = new ArrayList<>();
      List<Contact> newEmailContacts = new ArrayList<>();
      for (int i = 0; i < incomingEmailContacts.size(); i++) {
        EmailContact incomingEmail = incomingEmailContacts.get(i);
        int incomingId = incomingEmail.getId();
        if (incomingId <= 0) {
          Contact contact =
              buildEmailContact(
                  incomingEmail.getName(), incomingEmail.getEmail(), userName, createdTime);
          newEmailContacts.add(contact);
          newEmails.add(incomingEmail);
        } else {
          incomingContactIds.add(incomingId);
        }
      }

      if (!newEmailContacts.isEmpty()) {
        int[] result =
            contactStore.createContact(alert.getNamespaceId(), newEmailContacts, connection);
        for (int i = 0; i < result.length; i++) {
          if (result[i] == 0) {
            throw internalServerError("One or more requests failed adding contacts to alert");
          }
          Contact contact = newEmailContacts.get(i);
          int id = contact.getId();
          incomingContactIds.add(id);

          EmailContact newEmailContact = newEmails.get(i);
          newEmailContact.setId(id);
        }
      }
    }

    List<SlackContact> slackContacts = batchContact.getSlack();
    if (slackContacts != null) {
      for (SlackContact slackContact : slackContacts) {
        incomingContactIds.add(slackContact.getId());
      }
    }

    List<OpsGenieContact> opsGenieContacts = batchContact.getOpsgenie();
    if (opsGenieContacts != null) {
      for (OpsGenieContact opsGenieContact : opsGenieContacts) {
        incomingContactIds.add(opsGenieContact.getId());
      }
    }

    List<HttpContact> httpContacts = batchContact.getHttp();
    if (httpContacts != null) {
      for (HttpContact httpContact : httpContacts) {
        incomingContactIds.add(httpContact.getId());
      }
    }

    List<OCContact> ocContacts = batchContact.getOc();
    if (ocContacts != null) {
      for (OCContact ocContact : ocContacts) {
        incomingContactIds.add(ocContact.getId());
      }
    }

    List<PagerDutyContact> pagerDutyContacts = batchContact.getPagerduty();
    if (pagerDutyContacts != null) {
      for(PagerDutyContact pagerDutyContact: pagerDutyContacts) {
        incomingContactIds.add(pagerDutyContact.getId());
      }
    }


    List<Integer> idsToAdd = new ArrayList<>(incomingContactIds);
    List<Integer> existingContactIds = store.getContactIds(alert.getId(), connection);
    idsToAdd.removeAll(existingContactIds);

    if (!idsToAdd.isEmpty()) {
      int[] result = store.createAlertContact(alert.getId(), idsToAdd, connection);
      for (int i = 0; i < result.length; i++) {
        if (result[i] == 0) {
          throw internalServerError("One or more requests failed adding contacts to alert");
        }
      }
    }

    existingContactIds.removeAll(incomingContactIds);
    if (!existingContactIds.isEmpty()) {
      int[] result = store.deleteAlertContactByAlert(alert.getId(), existingContactIds, connection);
      for (int i = 0; i < result.length; i++) {
        if (result[i] == 0) {
          throw internalServerError("One or more contacts failed removing from alert");
        }
      }
    }
  }

  private Contact buildEmailContact(
      String name, String emailId, String createdBy, Timestamp createdTime) {
    Contact contact = new Contact();
    contact.setName(name == null ? emailId : name);
    contact.getDetails().put("email", emailId);
    contact.setType(ContactType.email);
    contact.setCreatedBy(createdBy);
    contact.setCreatedTime(createdTime);
    contact.setUpdatedBy(createdBy);
    contact.setUpdatedTime(createdTime);
    return contact;
  }

  @Override
  protected void doUpdates(List<Alert> alerts, Connection connection)
      throws IOException, SQLException {
    List<Alert> updateAlerts = new ArrayList<>();
    if (!alerts.isEmpty()) {
      for (Alert alert : alerts) {
        updateAlerts.add(getAndUpdate(alert, connection));
      }
      if (!updateAlerts.isEmpty()) {
        int[] result = store.update(updateAlerts, connection);
        for (Alert alert : updateAlerts) {
          addToAlertContact(alert, connection);
        }
        for (int i = 0; i < result.length; i++) {
          if (result[i] == 0) {
            throw internalServerError("One of more updates failed");
          }
        }
      }
    }
  }

  public void deleteById(Namespace namespace, long[] ids, String principal) {
    authorize(namespace, principal);
    try (Connection connection = store.getReadWriteConnection()) {
      try {
        store.softDelete(ids, principal, connection);
        store.removeContactsFromAlert(ids, connection);
        store.commit(connection);
      } catch (Exception e) {
        store.rollback(connection);
        throw e;
      }
    } catch (SQLException e) {
      String message = "Error deleting alerts";
      logger.error(message);
      throw internalServerError(message);
    }
  }

  public void restore(Namespace namespace, long[] ids, String principal) {
    authorize(namespace, principal);
    try (Connection connection = store.getReadWriteConnection()) {
      try {
        store.restore(ids, principal, connection);
        store.commit(connection);
      } catch (SQLException e) {
        store.rollback(connection);
        throw e;
      }
    } catch (SQLException e) {
      String message = "Error restoring alerts";
      logger.error(message);
      throw internalServerError(message);
    }
  }

  private Alert getAndUpdate(Alert modifiedAlert, Connection connection)
      throws IOException, SQLException {
    Alert originalAlert = store.get(modifiedAlert.getId(), true, false, connection);
    if (null == originalAlert) {
      throw notFoundException("Alert not found with id: " + modifiedAlert.getId());
    }
    updateFields(originalAlert, modifiedAlert);
    return originalAlert;
  }

  private void updateFields(Alert originalAlert, Alert modifiedAlert) {
    if (!isNullOrEmpty(modifiedAlert.getName())) {
      originalAlert.setName(modifiedAlert.getName());
    }

    if (modifiedAlert.getLabels() != null) {
      originalAlert.setLabels(modifiedAlert.getLabels());
    }

    Map<String, Object> newDefinition = modifiedAlert.getDefinition();
    if (newDefinition != null) {
      Map<String, Object> originalDefinition = originalAlert.getDefinition();

      Object newNotification = newDefinition.get(NOTIFICATION);
      if (newNotification != null) {
        originalDefinition.put(NOTIFICATION, newNotification);
      }

      Object newQueries = newDefinition.get(QUERIES);
      if (newQueries != null) {
        originalDefinition.put(QUERIES, newQueries);
      }

      Object newThreshold = newDefinition.get(THRESHOLD);
      if (newThreshold != null) {
        originalDefinition.put(THRESHOLD, newThreshold);
      }

      Object newGroupingRules = newDefinition.get(GROUPING_RULES);
      if (newGroupingRules != null) {
        originalDefinition.put(GROUPING_RULES, newGroupingRules);
      }

      int newVersion = modifiedAlert.getVersion();
      if (newVersion > NOT_PASSED) {
        originalDefinition.put(VERSION, newVersion);
      }

      Object newCreatedForm = newDefinition.get(CREATED_FROM);
      if (newCreatedForm != null) {
        originalDefinition.put(CREATED_FROM, newCreatedForm);
      }
    }

    if (modifiedAlert.getContacts() != null) {
      originalAlert.setContacts(modifiedAlert.getContacts());
    }

    if (modifiedAlert.isEnabled() != null) {
      originalAlert.setEnabled(modifiedAlert.isEnabled());
    }

    originalAlert.setUpdatedBy(modifiedAlert.getUpdatedBy());
    originalAlert.setUpdatedTime(modifiedAlert.getUpdatedTime());
  }

  public AlertView getById(final long id, final boolean fetchDefinition, final boolean deleted) {
    final String format = "Error reading alert by id: %d";
    AlertView view =
        get(
            (connection) -> {
              Alert alert = store.get(id, fetchDefinition, deleted, connection);
              if (alert == null) {
                throw notFoundException("Alert not found with id: " + id);
              }
              List<Contact> contacts = contactStore.getContactsForAlert(id, connection);
              alert.setContactList(contacts);
              return alert;
            },
            format,
            id);
    return view;
  }

  public List<AlertView> getByNamespace(
      final String namespaceName, final boolean fetchDefinition, final boolean deleted) {
    Namespace namespace;
    try {
      namespace = namespaceCache.getByName(namespaceName);
    } catch (Exception e) {
      String message = "Error reading namespace with name: " + namespaceName;
      logger.error(message, e);
      throw internalServerError(message);
    }
    validateNamespace(namespace, namespaceName);
    int namespaceId = namespace.getId();
    final String format = "Error listing alerts for namespace: %s";
    List<AlertView> views =
        list(
            (connection) -> {
              List<Alert> alerts = store.get(namespaceId, fetchDefinition, deleted, connection);
              for (Alert alert : alerts) {
                List<Contact> contacts =
                    contactStore.getContactsForAlert(alert.getId(), connection);
                alert.setContactList(contacts);
              }
              return alerts;
            },
            format,
            namespace.getName());
    return views;
  }

  public AlertView getByNamespaceAndName(
      final String namespaceName,
      final String name,
      final boolean fetchDefinition,
      final boolean deleted) {
    Namespace namespace;
    try {
      namespace = namespaceCache.getByName(namespaceName);
    } catch (Exception e) {
      String message = "Error reading namespace with name: " + namespaceName;
      logger.error(message, e);
      throw internalServerError(message);
    }
    validateNamespace(namespace, namespaceName);
    int namespaceId = namespace.getId();
    final String format = "Error reading alert for namespaceId: %d and name: %s";
    AlertView view =
        get(
            (connection) -> {
              Alert alert = store.get(namespaceId, name, fetchDefinition, deleted, connection);
              if (alert == null) {
                throw notFoundException(
                    "Alert not found for namespace: " + namespaceName + " name: " + name);
              }
              List<Contact> contacts = contactStore.getContactsForAlert(alert.getId(), connection);
              alert.setContactList(contacts);
              return alert;
            },
            format,
            namespaceId,
            namespaceName);
    return view;
  }

  @Override
  protected void setCreatorUpdatorIdAndTime(Alert alert, String principal, Timestamp timestamp) {
    alert.setCreatedBy(principal);
    alert.setCreatedTime(timestamp);
    setUpdaterIdAndTime(alert, principal, timestamp);
  }

  @Override
  protected void setUpdaterIdAndTime(Alert alert, String principal, Timestamp timestamp) {
    alert.setUpdatedBy(principal);
    alert.setUpdatedTime(timestamp);
  }

  @Override
  protected void preCreate(Alert alert) {
    Map<String, Object> definition = alert.getDefinition();
    if (isNullOrEmpty(definition)
        || !definition.containsKey(QUERIES)
        || !definition.containsKey(THRESHOLD)
        || !definition.containsKey(NOTIFICATION)
        || !definition.containsKey(GROUPING_RULES)) {
      throw badRequestException("Alert Definition is required");
    }
    int version = alert.getVersion();
    if (version <= NOT_PASSED) {
      version = DEFAULT_VERSION;
      alert.setVersion(version);
    }
    definition.put(VERSION, version);
  }
}
