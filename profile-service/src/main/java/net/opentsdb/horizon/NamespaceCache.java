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

package net.opentsdb.horizon;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.store.NamespaceStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

public class NamespaceCache {

  private static Logger logger = LoggerFactory.getLogger(NamespaceCache.class);

  private NamespaceStore namespaceStore;
  private Cache<Integer, Namespace> namespaceCache;
  private Cache<String, Namespace> namespaceCacheByName;

  public NamespaceCache(final CacheConfig cacheConfig, final NamespaceStore namespaceStore) {
    this.namespaceStore = namespaceStore;
    this.namespaceCache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(cacheConfig.namespaceTTL, cacheConfig.namespaceTTLUnit)
            .build();
    this.namespaceCacheByName =
        CacheBuilder.newBuilder()
            .expireAfterWrite(cacheConfig.namespaceTTL, cacheConfig.namespaceTTLUnit)
            .build();
  }

  public Namespace getByName(String namespace) throws Exception {
    try {
      String lowerCase = namespace.toLowerCase();
      return namespaceCacheByName.get(lowerCase, () -> loadNamespace(lowerCase));
    } catch (CacheLoader.InvalidCacheLoadException e) {
      logger.error(e.getMessage());
      return null;
    }
  }

  public Namespace getById(int id) throws Exception {
    try {
      return namespaceCache.get(id, () -> loadNamespace(id));
    } catch (CacheLoader.InvalidCacheLoadException e) {
      logger.error(e.getMessage());
      return null;
    }
  }

  private Namespace loadNamespace(int id) throws Exception {
    try (Connection connection = namespaceStore.getReadOnlyConnection()) {
      Namespace namespace = namespaceStore.getById(id, connection);
      logger.debug("Namespace cache missed for id: {}", id);
      return namespace;
    }
  }

  private Namespace loadNamespace(String namespaceName) throws Exception {
    try (Connection connection = namespaceStore.getReadOnlyConnection()) {
      Namespace namespace = namespaceStore.getNamespaceByName(namespaceName, connection);
      logger.debug("Namespace cache refreshed with name {}", namespaceName);
      return namespace;
    }
  }
}
