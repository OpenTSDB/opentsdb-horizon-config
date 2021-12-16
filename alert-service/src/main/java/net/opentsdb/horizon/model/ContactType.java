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

public enum ContactType {
  email((byte) 0),
  slack((byte) 1),
  opsgenie((byte) 2),
  http((byte) 3),
  oc((byte) 4),
  pagerduty((byte) 5);

  private final byte id;

  ContactType(byte typeidx) {
    this.id = typeidx;
  }

  public byte getId() {
    return id;
  }

  public static ContactType getById(int id) {
    switch (id) {
      case 0:
        return email;
      case 1:
        return slack;
      case 2:
        return opsgenie;
      case 3:
        return http;
      case 4:
        return oc;
      case 5:
        return pagerduty;
      default:
        throw new IllegalStateException("No ContactType for id " + id);
    }

  }
}
