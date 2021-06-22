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

package net.opentsdb.horizon.profile;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UtilsTest {

  private static List<String> athensRoles =
      Arrays.asList(
          "monitoring.tenant.d1.admin",
          "monitoring.tenant.foo.res_group.namespace_media_api-tw.access",
          "monitoring.tenant.foo.res_group.namespace_media_api-us.access",
          "monitoring.tenant.bar.res_group.namespace_trend-tw.access",
          "admin");

  @Test
  void parseTenantDomainsFromResourceGroupRoles() {

    HashSet expected =
        new HashSet() {
          {
            add("foo");
            add("bar");
          }
        };
    assertEquals(expected, Utils.parseTenantDomains(athensRoles));
  }

  @Test
  void parseNamespacesFromResourceGroupRoles() {
    HashSet expected =
        new HashSet() {
          {
            add("media_api-tw");
            add("media_api-us");
            add("trend-tw");
          }
        };
    assertEquals(expected, Utils.parseNamespaces(athensRoles));
  }
}
