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
import net.opentsdb.horizon.converter.SnoozeConverter;
import net.opentsdb.horizon.model.Contact;
import net.opentsdb.horizon.model.ContactType;
import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.model.Snooze;
import net.opentsdb.horizon.store.ContactStore;
import net.opentsdb.horizon.store.SnoozeStore;
import net.opentsdb.horizon.view.BatchContact;
import net.opentsdb.horizon.view.EmailContact;
import net.opentsdb.horizon.view.PagerDutyContact;
import net.opentsdb.horizon.view.SlackContact;
import net.opentsdb.horizon.view.SnoozeView;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static net.opentsdb.horizon.converter.AlertConverter.NOTIFICATION;
import static net.opentsdb.horizon.converter.AlertConverter.RECIPIENTS;
import static net.opentsdb.horizon.converter.SnoozeConverter.ALERTIDS;
import static net.opentsdb.horizon.converter.SnoozeConverter.FILTER;
import static net.opentsdb.horizon.converter.SnoozeConverter.LABELS;
import static net.opentsdb.horizon.converter.SnoozeConverter.REASON;
import static net.opentsdb.horizon.profile.Utils.validateNamespace;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.EMPTY_MAP;

public class SnoozeService extends AuthenticatedBaseService<SnoozeView, Snooze, SnoozeConverter> {

  public static final String SO_SERVICE = "HZ_SNOOZE_SERVICE";

  private final ContactStore contactStore;
  private final SnoozeStore snoozeStore;

  public SnoozeService(
      final SnoozeStore snoozeStore,
      final AuthService authService,
      final NamespaceCache namespaceCache,
      final ContactStore contactStore) {
    super(new SnoozeConverter(namespaceCache), snoozeStore, authService, namespaceCache);
    this.contactStore = contactStore;
    this.snoozeStore = snoozeStore;
  }

  @Override
  protected void doCreates(
      final List<Snooze> snoozeList, final Connection connection, final String principal)
      throws IOException, SQLException {

    final long compareTimeMillis = System.currentTimeMillis();

    validate(snoozeList, compareTimeMillis);
    // Put the missing fields

    setProperStartTime(snoozeList, compareTimeMillis);

    if (!snoozeList.isEmpty()) {
      snoozeStore.creates(snoozeList, connection);
    }
    for (Snooze snooze : snoozeList) {
      addToSnoozeContact(snooze, connection, principal);
    }
  }

  private void setProperStartTime(final List<Snooze> snoozeList, final long currentTimeMillis) {
    snoozeList.stream()
        .forEach(
            snooze -> {
              final Timestamp startTime =
                  (snooze.getStartTime() == null)
                      ? new Timestamp(currentTimeMillis)
                      : snooze.getStartTime();

              snooze.setStartTime(startTime);
              snooze.setEndTime(snooze.getEndTime());
            });
  }

  /**
   * If this method is called from update flow, it is expected that the get and reconcile have
   * already happened.
   *
   * @param snoozeList
   * @param compareTimeMillis
   */
  private void validate(final List<Snooze> snoozeList, final long compareTimeMillis) {

    if (invalidContactList(snoozeList)) {
      // We can optionally choose to partially create snoozes
      // Though its not palatable without proper handling
      throw badRequestException("One or more snoozes have misconfigured notification types");
    }

    final String invalidEndTime;
    if ((invalidEndTime = invalidStartEndTimes(snoozeList, compareTimeMillis)) != null) {
      throw badRequestException(invalidEndTime);
    }

    final String invalidFilters;
    if ((invalidFilters = invalidFilters(snoozeList)) != null) {
      throw badRequestException(invalidFilters);
    }
  }

  private String invalidFilters(List<Snooze> snoozeList) {

    final boolean allFiltersNull =
        snoozeList.stream()
            .anyMatch(
                snooze -> {
                  final Map<String, Object> definition = snooze.getDefinition();

                  if (definition == null) {
                    return true;
                  }

                  final Map<String, Object> filterMap =
                      (Map<String, Object>)
                          Optional.ofNullable(definition.get(FILTER)).orElse(EMPTY_MAP);

                  final List<String> labels =
                      (List<String>) Optional.ofNullable(definition.get(LABELS)).orElse(EMPTY_LIST);

                  final List<Integer> alertIds =
                      (List<Integer>)
                          Optional.ofNullable(definition.get(ALERTIDS)).orElse(EMPTY_LIST);

                  if (filterMap.isEmpty() && labels.isEmpty() && alertIds.isEmpty()) {
                    return true;
                  }

                  return false;
                });

    if (allFiltersNull) {
      return "One or more Snoozes have no selection criteria specified. "
          + "Atleast of no alertIds, labels or filter should be specified and valid";
    }

    return null;
  }

  private boolean invalidContactList(List<Snooze> snoozeList) {
    return snoozeList.stream()
        .anyMatch(
            snooze ->
                (Objects.nonNull(snooze.getBatchContact())
                    && (Objects.nonNull(snooze.getBatchContact().getHttp())
                        || Objects.nonNull(snooze.getBatchContact().getOc())
                        || Objects.nonNull(snooze.getBatchContact().getOpsgenie()))));
  }

  private String invalidStartEndTimes(List<Snooze> snoozeList, long compareTimeMillis) {
    for (Snooze snooze : snoozeList) {

      if (snooze.getEndTime() == null) {
        return "One or more Snoozes have a null endTime";
      } else if (snooze.getEndTime().getTime() <= compareTimeMillis) {
        return "One or more Snoozes have an endTime in the past";
      } else if (snooze.getStartTime() != null) {
        if (snooze.getStartTime().getTime() >= snooze.getEndTime().getTime()) {
          return "One or more Snoozes have an endTime, which is less than startTime";
        }
      }
    }
    return null;
  }

  @Override
  protected void doUpdates(final List<Snooze> snoozeList, final Connection connection)
      throws SQLException, IOException {
    final long currentTimeMillis = System.currentTimeMillis();
    if (!snoozeList.isEmpty()) {
      List<Snooze> updateSnoozeList = new ArrayList<>();
      for (Snooze snooze : snoozeList) {
        updateSnoozeList.add(getAndUpdate(snooze, connection));
      }
      validate(updateSnoozeList, currentTimeMillis);

      if (!updateSnoozeList.isEmpty()) {
        int[] result = snoozeStore.update(updateSnoozeList, connection);
        boolean badResult = IntStream.of(result).anyMatch(x -> x == 0);
        if (badResult) {
          throw internalServerError("One of more updates failed");
        }
        for (Snooze snooze : updateSnoozeList) {
          snoozeStore.deleteSnoozeContactBySnoozeId(snooze.getId(), connection);
          addToSnoozeContact(snooze, connection, snooze.getUpdatedBy());
        }
      }
    }
  }

  private int[] addToSnoozeContact(Snooze snooze, Connection connection, String principal)
      throws IOException, SQLException {
    List<Integer> contactids = new ArrayList<>();
    BatchContact batchContact = snooze.getBatchContact();
    int namespaceId = snooze.getNamespaceId();

    if (Objects.isNull(batchContact)) {
      return new int[0];
    }

    List<Contact> contacts = contactStore.getContactsByNamespace(namespaceId, connection);
    Map<String, Integer> contactMap =
        contacts.stream()
            .collect(
                Collectors.toMap(contact -> contact.getType() + contact.getName(), Contact::getId));

    Timestamp createdTime = new Timestamp(System.currentTimeMillis());

    List<EmailContact> emailContacts = batchContact.getEmail();
    if (emailContacts != null) {
      for (EmailContact emailContact : emailContacts) {
        String name = emailContact.getName();
        Integer contactId = contactMap.get(ContactType.email + name);
        if (contactId == null) {
          Contact contact = new Contact();
          contact.setName(name);
          String email = emailContact.getEmail();
          contact.getDetails().put("email", email == null ? name : email);
          contact.setType(ContactType.email);
          contact.setCreatedBy(principal);
          contact.setCreatedTime(createdTime);
          contact.setUpdatedBy(principal);
          contact.setCreatedTime(createdTime);
          logger.info("Creating a new contact because it doesn't exist " + contact);
          contactStore.createContact(namespaceId, contact, connection);
          contactId = contact.getId();
        }
        contactids.add(contactId);
      }
    }
    /**
     * List<EmailContact> emailContacts = batchContact.getEmail();
     *
     * <p>if(emailContacts != null) { for(EmailContact emailContact : emailContacts) { Integer
     * contactId = contactMap.get(ContactType.email + emailContact.getName()); if (contactId ==
     * null) { throw new IllegalStateException("Contact does not exist" + emailContact); } else {
     * contactids.add(contactId); } } }
     */
    List<SlackContact> slackContacts = batchContact.getSlack();

    if (slackContacts != null) {
      for (SlackContact slackContact : slackContacts) {
        Integer contactId = contactMap.get(ContactType.slack + slackContact.getName());
        if (contactId == null) {
          throw new IllegalStateException("Contact does not exist" + slackContact);
        } else {
          contactids.add(contactId);
        }
      }
    }

    List<PagerDutyContact> pagerDutyContacts = batchContact.getPagerduty();
    if (pagerDutyContacts != null) {
      for (PagerDutyContact pagerDutyContact : pagerDutyContacts) {
        Integer contactId = contactMap.get(ContactType.pagerduty + pagerDutyContact.getName());
        if (contactId == null) {
          throw new IllegalStateException("Contact does not exist" + pagerDutyContact);
        } else {
          contactids.add(contactId);
        }
      }
    }

    int[] result = snoozeStore.createSnoozeContact(snooze.getId(), contactids, connection);
    for (int i = 0; i < result.length; i++) {
      if (result[i] == 0) {
        throw internalServerError("One or more requests failed adding contacts to Snooze");
      }
    }
    return result;
  }

  public void deleteByIds(final long[] ids, final String principal) {

    try (Connection connection = snoozeStore.getReadWriteConnection()) {

      // Best to make a get batch api
      // Avoiding lammbdas for exception handling
      try {
        Set<Namespace> namespaceSet = new HashSet();
        for (long id : ids) {
          final Snooze byId = snoozeStore.getById(id, false, connection);
          if (byId == null) {
            throw notFoundException("Snooze not found with id: " + id);
          }
          int namespaceId = byId.getNamespaceId();
          Namespace namespace;
          try {
            namespace = namespaceCache.getById(namespaceId);
          } catch (Exception e) {
            snoozeStore.rollback(connection);
            String message =
                "Error deleting snooze, failed to read namespace with id: " + namespaceId;
            logger.error(message, e);
            throw internalServerError(message);
          }
          validateNamespace(namespace, namespaceId);
          namespaceSet.add(namespace);
        }

        for (Namespace namespace : namespaceSet) {
          authorize(namespace, principal);
        }

        snoozeStore.deleteSnoozeContactBySnoozeId(ids, connection);
        snoozeStore.delete(ids, connection);
        snoozeStore.commit(connection);
      } catch (SQLException | IOException e) {
        snoozeStore.rollback(connection);
        throw e;
      }
    } catch (SQLException | IOException e) {
      String message = "Error deleting snooze";
      logger.error(message, e);
      throw internalServerError(message);
    }
  }

  private Snooze getAndUpdate(Snooze modifiedSnooze, Connection connection)
      throws IOException, SQLException {
    Snooze snooze = snoozeStore.getById(modifiedSnooze.getId(), false, connection);
    if (snooze == null) {
      throw notFoundException("Snooze not found with id: " + modifiedSnooze.getId());
    }

    Map<String, Object> originalDefinition = snooze.getDefinition();

    if (originalDefinition.containsKey(NOTIFICATION)) {
      final Object notification = originalDefinition.get(NOTIFICATION);
      Object contact = ((Map<String, Object>) notification).get(RECIPIENTS);
      snooze.setContact(contact);
      BatchContact batchContact =
          AlertConverter.OBJECT_MAPPER.convertValue(contact, BatchContact.class);
      snooze.setBatchContact(batchContact);
    }
    final Map<String, Object> modifiedSnoozeDefinition = modifiedSnooze.getDefinition();

    // We have taken care of other conditions before.

    snooze.setStartTime(
        Optional.ofNullable(modifiedSnooze.getStartTime()).orElse(snooze.getStartTime()));

    snooze.setEndTime(Optional.ofNullable(modifiedSnooze.getEndTime()).orElse(snooze.getEndTime()));

    snooze.setUpdatedBy(
        Optional.ofNullable(modifiedSnooze.getUpdatedBy()).orElse(snooze.getUpdatedBy()));

    snooze.setUpdatedTime(
        Optional.ofNullable(modifiedSnooze.getUpdatedTime()).orElse(snooze.getUpdatedTime()));

    checkAndReplace(originalDefinition, modifiedSnoozeDefinition, FILTER);
    checkAndReplace(originalDefinition, modifiedSnoozeDefinition, LABELS);
    checkAndReplace(originalDefinition, modifiedSnoozeDefinition, ALERTIDS);
    checkAndReplace(originalDefinition, modifiedSnoozeDefinition, NOTIFICATION);
    checkAndReplace(originalDefinition, modifiedSnoozeDefinition, REASON);

    return snooze;
  }

  private void checkAndReplace(
      final Map<String, Object> originalDefinition,
      final Map<String, Object> modifiedSnoozeDefinition,
      final String field) {
    if (modifiedSnoozeDefinition.containsKey(field)) {
      originalDefinition.put(field, modifiedSnoozeDefinition.get(field));
    }
  }

  public SnoozeView getById(final long id) {
    final String format = "Error reading snooze by id: %d";
    SnoozeView snoozeView =
        get((connection) -> snoozeStore.getById(id, false, connection), format, id);
    if (snoozeView == null || isExpired(snoozeView)) {
      throw notFoundException("Snooze not found with id: " + id);
    }
    return snoozeView;
  }

  private static boolean isExpired(SnoozeView snoozeView) {
    return snoozeView.getEndTime().getTime() <= System.currentTimeMillis();
  }

  public List<SnoozeView> getForNamespace(final String namespaceName) {
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
    final String format = "Error listing snooze for namespace: %s";
    List<SnoozeView> snoozeViews =
        list(
            (connection) -> snoozeStore.getForNamespace(namespaceId, false, connection),
            format,
            namespace.getName());
    final List<SnoozeView> activeSnoozeViews =
        snoozeViews.stream()
            .filter(snoozeView -> !isExpired(snoozeView))
            .collect(Collectors.toList());
    return activeSnoozeViews;
  }

  @Override
  protected void setCreatorIdAndTime(Snooze snooze, String principal, Timestamp timestamp) {
    snooze.setCreatedBy(principal);
    snooze.setCreatedTime(timestamp);
  }

  @Override
  protected void setUpdaterIdAndTime(Snooze snooze, String principal, Timestamp timestamp) {
    snooze.setUpdatedBy(principal);
    snooze.setUpdatedTime(timestamp);
  }
}
