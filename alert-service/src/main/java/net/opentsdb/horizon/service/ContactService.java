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
import net.opentsdb.horizon.model.User;
import net.opentsdb.horizon.converter.BaseConverter;
import net.opentsdb.horizon.converter.BatchContactConverter;
import net.opentsdb.horizon.model.Contact;
import net.opentsdb.horizon.model.ContactType;
import net.opentsdb.horizon.store.ContactStore;
import net.opentsdb.horizon.util.Utils;
import net.opentsdb.horizon.view.BatchContact;
import net.opentsdb.horizon.view.EmailContact;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static net.opentsdb.horizon.profile.Utils.validateNamespace;
import static net.opentsdb.horizon.converter.BatchContactConverter.EMAIL;
import static net.opentsdb.horizon.util.Utils.isNullOrEmpty;

public class ContactService
    extends AuthenticatedBaseService<BatchContact, List<Contact>, BatchContactConverter> {

  private final NamespaceMemberService namespaceMemberService;
  private ContactStore store;
  private String adminEmailDomain;

  public ContactService(
      final ContactStore contactStore,
      final AuthService authService,
      final NamespaceCache namespaceCache,
      final NamespaceMemberService namespaceMemberService,
      final String adminEmailDomain) {

    super(new BatchContactConverter(), contactStore, authService, namespaceCache);
    this.store = contactStore;
    this.namespaceMemberService = namespaceMemberService;
    this.adminEmailDomain = adminEmailDomain;
  }

  @Override
  protected void doCreate(final List<Contact> contacts, final Connection connection)
      throws IOException, SQLException {

    if (!contacts.isEmpty()) {
      int namespaceId = contacts.get(0).getNamespaceid();

      if (namespaceId == BaseConverter.NOT_PASSED) {
        throw badRequestException("NamespaceId not found");
      }

      int[] result = store.createContact(namespaceId, contacts, connection);
      for (int i = 0; i < result.length; i++) {
        if (result[i] == 0) {
          throw internalServerError("One or more creates failed");
        }
      }
    }
  }

  @Override
  protected List<Contact> doUpdate(List<Contact> contacts, Connection connection)
      throws IOException, SQLException {

    List<Contact> updateContacts = new ArrayList<>();
    for (Contact contact : contacts) {
      updateContacts.add(getAndUpdate(contact, connection));
    }

    if (!updateContacts.isEmpty()) {
      int[] result = store.update(updateContacts, connection);
      for (int i = 0; i < result.length; i++) {
        if (result[i] == 0) {
          throw internalServerError("One or more updates failed");
        }
      }
    }
    return updateContacts;
  }

  @Override
  protected void doDelete(List<Contact> contacts, Connection connection) throws SQLException {

    List<Integer> ids = new ArrayList<>();
    List<Contact> contactsWithoutIds = new ArrayList<>();
    for (int i = 0; i < contacts.size(); i++) {
      Contact contact = contacts.get(i);
      int id = contact.getId();
      if (id > 0) {
        ids.add(id);
      } else {
        contactsWithoutIds.add(contact);
      }
    }

    if (!ids.isEmpty()) {
      int[] result = store.deleteByIds(ids, connection);
      for (int i = 0; i < result.length; i++) {
        if (result[i] == 0) {
          throw internalServerError("one or more deletes failed");
        }
      }
    }
    if (!contactsWithoutIds.isEmpty()) {
      int[] result = store.delete(contactsWithoutIds, connection);
      for (int i = 0; i < result.length; i++) {
        if (result[i] == 0) {
          throw internalServerError("one or more deletes failed");
        }
      }
    }
  }

  public BatchContact getContactForAlert(long alertId) {
    final String format = "Error reading contact for alertId: %d";
    return get((connection) -> store.getContactsForAlert(alertId, connection), format, alertId);
  }

  public BatchContact getContactsByNamespaceAndType(String namespaceName, ContactType type) {
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
    final String format = "Error reading contact for namespaceId: %d and type: %s";
    BatchContact batchContact =
        get(
            (connection) -> store.getContactByType(namespaceId, type, connection),
            format,
            namespaceId,
            type);

    if (type == ContactType.email) {
      addAdminContacts(namespaceId, batchContact);
    }
    return batchContact;
  }

  public BatchContact getContactsByNamespace(String namespaceName) {
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
    final String format = "Error reading contact for namespaceId: %d";
    BatchContact batchContact =
        get(
            (connection) -> store.getContactsByNamespace(namespaceId, connection),
            format,
            namespaceId);

    addAdminContacts(namespaceId, batchContact);
    return batchContact;
  }

  private void addAdminContacts(int namespaceId, BatchContact batchContact) {
    List<EmailContact> emails = batchContact.getEmail();

    if (emails == null) {
      emails = new ArrayList<>();
      batchContact.setEmail(emails);
    }

    Set<EmailContact> adminEmails = buildAdminEmailContacts(namespaceId);

    for (int i = 0; i < emails.size(); i++) {
      EmailContact email = emails.get(i);
      if (adminEmails.contains(email)) {
        email.setAdmin(true);
        adminEmails.remove(email);
      }
    }
    emails.addAll(adminEmails);
  }

  private Set<EmailContact> buildAdminEmailContacts(final int namespaceId) {
    List<User> users = namespaceMemberService.getNamespaceMember(namespaceId);
    Set<EmailContact> adminEmails = new HashSet<>();
    for (int i = 0; i < users.size(); i++) {
      String userId = users.get(i).getUserid();
      EmailContact email = new EmailContact();
      email.setAdmin(true);
      String emailId = userId.substring(userId.indexOf(".") + 1) + adminEmailDomain;
      email.setEmail(emailId);
      email.setName(emailId);
      adminEmails.add(email);
    }
    return adminEmails;
  }

  private Contact getAndUpdate(Contact modifiedContact, Connection connection)
      throws IOException, SQLException {
    int id = modifiedContact.getId();
    Contact originalContact = store.getContactById(id, connection);
    if (originalContact == null) {
      throw notFoundException("Contact not found with id: " + id);
    }

    updateFields(originalContact, modifiedContact);
    return originalContact;
  }

  private void updateFields(Contact original, Contact modified) {
    String newName = modified.getName();
    if (!isNullOrEmpty(newName)) {
      original.setName(newName);
    }

    Map<String, String> newDetails = modified.getDetails();
    if (!isNullOrEmpty(newDetails)) {
      original.setDetails(newDetails);
    }

    original.setUpdatedBy(modified.getUpdatedBy());
    original.setUpdatedTime(modified.getUpdatedTime());
  }

  @Override
  protected void preCreate(List<Contact> contacts) {
    contacts.stream()
        .filter(contact -> contact.getType().equals(ContactType.email))
        .forEach(
            contact -> {
              if (Utils.isNullOrEmpty(contact.getName())) {
                Map<String, String> details = contact.getDetails();
                contact.setName(details.get(EMAIL));
              }
            });
  }

  @Override
  protected void preUpdate(List<Contact> contacts) {
    preCreate(contacts);
  }

  @Override
  protected void setCreatorUpdatorIdAndTime(
      List<Contact> contacts, String principal, Timestamp timestamp) {
    for (Contact contact : contacts) {
      contact.setCreatedBy(principal);
      contact.setCreatedTime(timestamp);
    }
    setUpdaterIdAndTime(contacts, principal, timestamp);
  }

  @Override
  protected void setUpdaterIdAndTime(
      List<Contact> contacts, String principal, Timestamp timestamp) {
    for (Contact contact : contacts) {
      contact.setUpdatedBy(principal);
      contact.setUpdatedTime(timestamp);
    }
  }
}
