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

package net.opentsdb.horizon.view;

public enum SourceType {
  DASHBOARD((byte) 1),
  ALERT((byte) 2),
  SNAPSHOT((byte) 3);

  public final byte id;

  SourceType(byte id) {
    this.id = id;
  }

  public static SourceType valueOf(byte id) {
    try {
      return SourceType.values()[id - 1];
    } catch (ArrayIndexOutOfBoundsException e) {
      throw new IllegalArgumentException("No SourceType found for id " + id);
    }
  }
}
