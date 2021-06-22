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

public enum AlertType {
  simple((byte) 0),
  composite((byte) 1),
  hostdown((byte) 2),
  healthcheck((byte) 3),
  event((byte)4);

  public final byte id;

  AlertType(byte id) {
    this.id = id;
  }

  public byte getId() {
    return id;
  }

  public static AlertType getById(int id) {
    switch (id) {
      case 0:
        return simple;
      case 1:
        return composite;
      case 2:
        return hostdown;
      case 3:
        return healthcheck;
      case 4:
        return event;
      default:
        throw new IllegalArgumentException("No AlertType for id " + id);
    }
  }

}
