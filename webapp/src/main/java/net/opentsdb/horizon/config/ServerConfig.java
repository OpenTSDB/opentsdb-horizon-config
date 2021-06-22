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

import net.opentsdb.horizon.ext.DefaultMetricRegistryFactory;
import net.opentsdb.horizon.filter.DebugAuthFilter;
import net.opentsdb.horizon.ssl.KeyStoreSSLContextFactory;

import java.util.Map;

import static net.opentsdb.horizon.handler.Slf4jAccessLogReceiver.FORMAT;

public class ServerConfig {

  public int port;
  public String sslContextFactoryClass = KeyStoreSSLContextFactory.class.getName();
  public String[] ciphers;
  public String authFilterClassName = DebugAuthFilter.class.getName();
  public String builderCustomizerClassName;

  public boolean corsEnabled;
  public String allowedOrigin;
  public Integer corsMaxAge;

  public boolean accesslogEnabled = true;
  public String accesslogFormat = FORMAT;
  public String accesslogLoggerName = "AccessLog";

  public boolean instrumentationEnabled = true;
  public String metricRegistryFactoryClassName = DefaultMetricRegistryFactory.class.getName();
  public String healthCheckServletFactoryClassName;

  public boolean swaggerEnabled;
  public String swaggerHome;
  public String swaggerHost;
  public String swaggerTitle;
  public String swaggerVersion;
  public String swaggerBasePath;
  public String swaggerResourcePackage = "net.opentsdb.horizon";

  public Map<String, Object> initParams;

}
