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

package net.opentsdb.horizon.config;

import net.opentsdb.horizon.CacheConfig;

import java.util.Map;

public class ApplicationConfig {

  public static final String ATHENZ_PRIVATE_KEY = "athenzPrivateKey";
  public static final String ATHENZ_PUBLIC_KEY = "athenzPublicKey";
  public static final String ATHENZ_TRUSTORE = "athenzTrustStore";
  public static final String ATHENZ_TRUSTORE_PASSWORD = "athenzTrustStorePassword";
  public static final String ATHENZ_SSLCONTEXT = "athenzSSlContext";

  public String keyReaderFactoryClassName;
  public String adminEmailDomain = "@";
  public CacheConfig cacheConfig;
  public String resourceExtenderClassName;
  public Map<String, Object> initParams;
}
