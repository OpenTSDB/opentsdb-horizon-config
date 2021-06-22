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

import net.opentsdb.horizon.model.Namespace;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;

import static net.opentsdb.horizon.service.NamespaceService.DH_TRACKS;

public class TestUtil {
  public static Namespace getNamespace() {
    Namespace namespace = new Namespace();
    namespace.setName("tsdbraw");
    namespace.setMeta(
        new HashMap<>() {
          {
            put(DH_TRACKS, Arrays.asList("tsdbrawraw"));
          }
        });
    namespace.setEnabled(true);
    namespace.setCreatedTime(new Timestamp(System.currentTimeMillis()));
    namespace.setUpdatedTime(new Timestamp(System.currentTimeMillis()));
    namespace.setCreatedBy("new-authorized-user");
    namespace.setUpdatedBy("new-authorized-user");
    return namespace;
  }

  public static Namespace getMockedNamespace() {
    Namespace namespace = new Namespace();
    namespace.setName("Mocked Namespace");
    namespace.setAlias("Mocked Alias");
    namespace.setMeta(
        new HashMap<>() {
          {
            put(DH_TRACKS, Arrays.asList("mocked track"));
          }
        });
    namespace.setEnabled(true);
    namespace.setId(7);
    return namespace;
  }
}
