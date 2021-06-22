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

import net.opentsdb.horizon.model.Contact;
import net.opentsdb.horizon.model.ContactType;
import net.opentsdb.horizon.view.BatchContact;
import net.opentsdb.horizon.view.EmailContact;
import net.opentsdb.horizon.view.HttpContact;
import net.opentsdb.horizon.view.OCContact;
import net.opentsdb.horizon.view.OpsGenieContact;
import net.opentsdb.horizon.view.SlackContact;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchContactConverter extends BaseConverter<BatchContact, List<Contact>> {

  public final static String EMAIL = "email";
  public final static String WEBHOOK = "webhook";
  public final static String ENDPOINT = "endpoint";
  public final static String APIKEY = "apikey";
  public final static String DISPLAY_COUNT = "displaycount";
  public final static String CUSTOMER = "customer";
  public final static String CONTEXT = "context";
  public final static String OPSDB_PROPERTY = "opsdbproperty";


  @Override
  public List<Contact> viewToModel(BatchContact batchContact) {
    int namespaceId = batchContact.getNamespaceId();
    List<Contact> contacts = new ArrayList<>();

    List<EmailContact> emailContacts = batchContact.getEmail();
    if (emailContacts != null) {
      for (EmailContact emailContact : emailContacts) {
        Contact contact = new Contact();
        contact.setId(emailContact.getId());
        contact.setName(emailContact.getName());
        contact.setNewName(emailContact.getNewname());
        contact.setType(ContactType.email);

        Map<String, String> details = new HashMap<>();
        details.put(EMAIL, emailContact.getEmail());
        contact.setDetails(details);
        contact.setNamespaceid(namespaceId);

        contacts.add(contact);
      }
    }

    List<SlackContact> slackContacts = batchContact.getSlack();
    if (slackContacts != null) {
      for (SlackContact slackContact : slackContacts) {
        Contact contact = new Contact();
        contact.setId(slackContact.getId());
        contact.setName(slackContact.getName());
        contact.setType(ContactType.slack);
        contact.setNewName(slackContact.getNewname());
        Map<String, String> details = new HashMap<>();
        details.put(WEBHOOK, slackContact.getWebhook());
        contact.setDetails(details);
        contact.setNamespaceid(namespaceId);

        contacts.add(contact);
      }
    }

    List<HttpContact> httpContacts = batchContact.getHttp();
    if (httpContacts != null) {
      for (HttpContact httpContact : httpContacts) {
        Contact contact = new Contact();
        contact.setId(httpContact.getId());
        contact.setName(httpContact.getName());
        contact.setType(ContactType.http);
        contact.setNewName(httpContact.getNewname());
        Map<String, String> details = new HashMap<>();
        details.put(ENDPOINT, httpContact.getEndpoint());
        contact.setDetails(details);
        contact.setNamespaceid(namespaceId);

        contacts.add(contact);
      }
    }

    List<OpsGenieContact> opsGenieContacts = batchContact.getOpsgenie();
    if (opsGenieContacts != null) {
      for (OpsGenieContact opsGenieContact : opsGenieContacts) {
        Contact contact = new Contact();
        contact.setId(opsGenieContact.getId());
        contact.setName(opsGenieContact.getName());
        contact.setType(ContactType.opsgenie);
        contact.setNewName(opsGenieContact.getNewname());
        Map<String, String> details = new HashMap<>();
        details.put(APIKEY, opsGenieContact.getApikey());
        contact.setDetails(details);
        contact.setNamespaceid(namespaceId);

        contacts.add(contact);
      }
    }

    List<OCContact> ocContacts = batchContact.getOc();
    if (ocContacts != null) {
      for (OCContact ocContact : ocContacts) {
        Contact contact = new Contact();
        contact.setId(ocContact.getId());
        contact.setName(ocContact.getName());
        contact.setNewName(ocContact.getNewname());

        contact.setType(ContactType.oc);
        Map<String, String> details = new HashMap<>();
        details.put(DISPLAY_COUNT, ocContact.getDisplaycount());
        details.put(CUSTOMER, ocContact.getCustomer());
        details.put(CONTEXT, ocContact.getContext());
        details.put(OPSDB_PROPERTY, ocContact.getOpsdbproperty());
        contact.setDetails(details);
        contact.setNamespaceid(namespaceId);

        contacts.add(contact);
      }
    }
    return contacts;
  }

  @Override
  public BatchContact modelToView(List<Contact> contacts) {
    BatchContact batchContact = new BatchContact();
    for (Contact contact : contacts) {
      String name = contact.getName();
      Map<String, String> details = contact.getDetails();

      if (contact.getType() == ContactType.email) {
        EmailContact emailContact = new EmailContact();
        emailContact.setId(contact.getId());
        emailContact.setName(name);
        emailContact.setEmail(details.get(EMAIL));

        List<EmailContact> emailContacts = batchContact.getEmail();
        if (emailContacts == null) {
          emailContacts = new ArrayList<>();
          batchContact.setEmail(emailContacts);
        }
        emailContacts.add(emailContact);

      } else if (contact.getType() == ContactType.slack) {
        SlackContact slackContact = new SlackContact();
        slackContact.setId(contact.getId());
        slackContact.setName(name);
        slackContact.setWebhook(details.get(WEBHOOK));

        List<SlackContact> slackContacts = batchContact.getSlack();
        if (slackContacts == null) {
          slackContacts = new ArrayList<>();
          batchContact.setSlack(slackContacts);
        }
        slackContacts.add(slackContact);

      } else if (contact.getType() == ContactType.opsgenie) {
        OpsGenieContact opsGenieContact = new OpsGenieContact();
        opsGenieContact.setId(contact.getId());
        opsGenieContact.setName(name);
        opsGenieContact.setApikey(contact.getDetails().get(APIKEY));

        List<OpsGenieContact> opsGenieContacts = batchContact.getOpsgenie();
        if (opsGenieContacts == null) {
          opsGenieContacts = new ArrayList<>();
          batchContact.setOpsgenie(opsGenieContacts);
        }
        opsGenieContacts.add(opsGenieContact);

      } else if (contact.getType() == ContactType.http) {
        HttpContact httpContact = new HttpContact();
        httpContact.setId(contact.getId());
        httpContact.setName(name);
        httpContact.setEndpoint(contact.getDetails().get(ENDPOINT));

        List<HttpContact> httpContacts = batchContact.getHttp();
        if (httpContacts == null) {
          httpContacts = new ArrayList<>();
          batchContact.setHttp(httpContacts);
        }
        httpContacts.add(httpContact);

      } else if (contact.getType() == ContactType.oc) {
        OCContact ocContact = new OCContact();
        ocContact.setId(contact.getId());
        ocContact.setName(name);
        ocContact.setCustomer(contact.getDetails().get(CUSTOMER));
        ocContact.setContext(contact.getDetails().get(CONTEXT));
        ocContact.setDisplaycount(contact.getDetails().get(DISPLAY_COUNT));
        ocContact.setOpsdbproperty(contact.getDetails().get(OPSDB_PROPERTY));

        List<OCContact> ocContacts = batchContact.getOc();
        if (ocContacts == null) {
          ocContacts = new ArrayList<>();
          batchContact.setOc(ocContacts);
        }
        ocContacts.add(ocContact);

      } else {
        throw new IllegalArgumentException("Not a valid contact type");
      }


    }
    return batchContact;
  }
}
