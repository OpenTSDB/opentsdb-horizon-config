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

import net.opentsdb.horizon.view.BaseDto;
import java.util.HashMap;
import java.util.Map;

public class Contact extends BaseDto {

  private int id;

  private String name;

  private String newName;

  private int namespaceid;

  private Map<String, String> details = new HashMap<>();

  private ContactType type;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public Map<String, String> getDetails() {
    return details;
  }

  public void setDetails(Map<String, String> details) {
    this.details = details;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getNewName() {
    return newName;
  }

  public void setNewName(String newName) {
    this.newName = newName;
  }

  public ContactType getType() {
    return type;
  }

  public void setType(ContactType type) {
    this.type = type;
  }

  public int getNamespaceid() {
    return namespaceid;
  }

  public void setNamespaceid(int namespaceid) {
    this.namespaceid = namespaceid;
  }

  @Override
  public String toString() {
    return "Contact{" +
        "name='" + name + '\'' +
        "newName='" + newName + '\'' +
        ", type=" + type +
        '}';
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Contact)) {
      return false;
    }
    Contact that = (Contact) obj;
    return this.name.equals(that.name) && this.type == that.type
        && this.namespaceid == that.namespaceid;
  }

  @Override
  public int hashCode() {
    return (this.name.hashCode() + this.type.name().hashCode() + this.namespaceid) * 31;
  }
}

