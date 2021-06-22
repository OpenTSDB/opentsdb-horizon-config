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
import net.opentsdb.horizon.converter.BatchContactConverter;
import net.opentsdb.horizon.model.Contact;
import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.store.ContactStore;
import net.opentsdb.horizon.store.NamespaceStore;
import net.opentsdb.horizon.view.BatchContact;
import net.opentsdb.horizon.view.EmailContact;
import net.opentsdb.horizon.view.OCContact;
import net.opentsdb.horizon.view.SlackContact;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import mockit.Tested;
import mockit.Verifications;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ContactServiceTest {

  @Tested private ContactService contactService;

  @Injectable private NamespaceStore namespaceStore;

  @Injectable private NamespaceMemberService namespaceMemberService;

  @Injectable private ContactStore contactStore;

  @Injectable private AuthService authService;

  @Injectable private NamespaceCache namespaceCache;

  @Injectable private String adminEmailDomain;

  @Mocked private Connection connection;

  @Mocked private HttpServletRequest httpServletRequest;


  @Test
  public void testAddContacts() throws SQLException, IOException {

    Namespace namespace = new Namespace();
    namespace.setId(1);
    namespace.setMeta(new HashMap<>());
    BatchContact batchContact = new BatchContact();
    batchContact.setNamespaceId(namespace.getId());
    new Expectations() {
      {
        authService.authorize(withInstanceOf(Namespace.class), anyString);
        result = true;
      }
    };

    EmailContact contact = new EmailContact();
    contact.setEmail("test@opentsdb.net");
    batchContact.setEmail(
        new ArrayList() {
          {
            add(contact);
          }
        });

    contactService.create(batchContact, namespace, "authorizeduser");

    new Verifications() {
      {
        List<Contact> capturedContact;
        contactStore.createContact(1, capturedContact = withCapture(), connection);
        times = 1;
        assertEquals(contact.getEmail(), capturedContact.get(0).getName());
      }
    };
  }

  @Test
  public void testGetAllContacts() throws Exception {
    EmailContact emailContact = new EmailContact();
    emailContact.setEmail("test@opentsdb.net");
    emailContact.setName("test@opentsdb.net");

    SlackContact slackContact = new SlackContact();
    slackContact.setName("myns");
    slackContact.setWebhook("opentsdb.net");

    OCContact ocContact = new OCContact();
    ocContact.setName("myns-oc");
    ocContact.setCustomer("customer");
    ocContact.setContext("live");
    ocContact.setDisplaycount("1");
    ocContact.setOpsdbproperty("myns");

    BatchContact batchContact = new BatchContact();
    batchContact.setEmail(
        new ArrayList<EmailContact>() {
          {
            add(emailContact);
          }
        });
    batchContact.setSlack(
        new ArrayList<SlackContact>() {
          {
            add(slackContact);
          }
        });
    batchContact.setOc(
        new ArrayList<OCContact>() {
          {
            add(ocContact);
          }
        });

    Namespace namespace = new Namespace();
    namespace.setId(1);
    namespace.setName("myns");
    batchContact.setNamespaceId(namespace.getId());

    new Expectations(namespaceCache) {
      {
        namespaceCache.getByName(anyString);
        result = namespace;
      }
    };
    BatchContactConverter contactConverter = new BatchContactConverter();
    final List<Contact> mockedList = contactConverter.viewToModel(batchContact);

    new Expectations(contactStore) {
      {
        contactStore.getContactsByNamespace(1, connection);
        result = mockedList;
      }
    };

    final BatchContact contactList = contactService.getContactsByNamespace("TestNamespace");
    assertNotNull(contactList);
    assertEquals(contactList.getEmail().size(), 1);
    assertEquals(contactList.getSlack().size(), 1);
    assertEquals(contactList.getOc().size(), 1);
  }

}
