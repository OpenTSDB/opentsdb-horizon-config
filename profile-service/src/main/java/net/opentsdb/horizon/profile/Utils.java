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

import net.opentsdb.horizon.model.Namespace;

import javax.ws.rs.NotFoundException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static net.opentsdb.horizon.service.NamespaceService.ATHENS_DOMAIN;

public class Utils {

  public static boolean isAthensManaged(Namespace namespace) {
    return getAthensDomain(namespace) != null;
  }

  public static String getAthensDomain(Namespace namespace) {
    return (String) namespace.getMeta().get(ATHENS_DOMAIN);
  }

  /**
   * Extracts the tenant domains from the resource group roles. The parsing logic is specific to the
   * role name format in Athens. It parses only the resource group role set during the tenancy setup
   * and discards others. The format of the resource groups roles in Athens is standardized and is
   * as follows.
   * <li>${service}.tenant.${tenantDomain}.res_group.namespace_${namespace}.${action}
   * <li>EX: monitoring.tenant.foo.res_group.namespace_media_api-tw.access
   */
  public static Set<String> parseTenantDomains(List<String> athensRoles) {

    Set<String> domains = new HashSet<>();
    for (String role : athensRoles) {
      String beginPattern = "monitoring.tenant.";
      int beginPatternLength = beginPattern.length();
      String resGroupPattern = ".res_group";
      if (role.contains(resGroupPattern)) {
        int beginIndex = role.indexOf(beginPattern) + beginPatternLength;
        int endIndex = role.indexOf(resGroupPattern);
        String domain = role.substring(beginIndex, endIndex);
        domains.add(domain);
      }
    }
    return domains;
  }

  /**
   * Extracts the namespace from the resource group roles. The parsing logic is specific to the role
   * name format in Athens. It parses only the resource group role set during the tenancy setup and
   * discards others. The format of the resource groups roles in Athens is standardized and is as
   * follows.
   * <li>${service}.tenant.${tenantDomain}.res_group.namespace_${namespace}.${action}
   * <li>EX: monitoring.tenant.foo.res_group.namespace_media_api-tw.access
   */
  public static Set<String> parseNamespaces(List<String> athensRoles) {
    Set<String> namespaces = new HashSet<>();
    for (String role : athensRoles) {
      String resGroupPattern = ".res_group.namespace_";
      int resGroupPatternLength = resGroupPattern.length();
      String actionPattern = ".access";
      if (role.contains(resGroupPattern)) {
        int beginIndex = role.indexOf(resGroupPattern) + resGroupPatternLength;
        int endIndex = role.indexOf(actionPattern);
        String namespace = role.substring(beginIndex, endIndex);
        namespaces.add(namespace);
      }
    }
    return namespaces;
  }

  public static void validateNamespace(Namespace namespace, String namespaceName) {
    if (null == namespace) {
      throw new NotFoundException("Namespace not found with name " + namespaceName);
    }
  }
  public static void validateNamespace(Namespace namespace, int namespaceId) {
    if (null == namespace) {
      throw new NotFoundException("Namespace not found with id " + namespaceId);
    }
  }
}
