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

package net.opentsdb.horizon.model;

import net.opentsdb.horizon.view.BatchContact;
import net.opentsdb.horizon.view.BaseDto;
import java.util.List;
import java.util.Map;
import javax.persistence.Transient;

public class Alert extends BaseDto {

  private long id;
  private String name;
  private int namespaceId;
  private AlertType type;
  private Boolean enabled;
  private boolean deleted;
  private List<String> labels;
  private Map<String, Object> definition;

  @Transient private BatchContact contacts;
  @Transient private List<Contact> contactList;
  @Transient private int version;

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getNamespaceId() {
    return namespaceId;
  }

  public void setNamespaceId(int namespaceId) {
    this.namespaceId = namespaceId;
  }

  public BatchContact getContacts() {
    return contacts;
  }

  public void setContacts(BatchContact contacts) {
    this.contacts = contacts;
  }

  public AlertType getType() {
    return type;
  }

  public void setType(AlertType type) {
    this.type = type;
  }

  public Boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(Boolean enabled) {
    this.enabled = enabled;
  }

  public boolean isDeleted() {
    return deleted;
  }

  public void setDeleted(final boolean deleted) {
    this.deleted = deleted;
  }

  public List<String> getLabels() {
    return labels;
  }

  public void setLabels(List<String> lables) {
    this.labels = lables;
  }

  public Map<String, Object> getDefinition() {
    return definition;
  }

  public void setDefinition(Map<String, Object> definition) {
    this.definition = definition;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public List<Contact> getContactList() {
    return contactList;
  }

  public void setContactList(List<Contact> contactList) {
    this.contactList = contactList;
  }
}
