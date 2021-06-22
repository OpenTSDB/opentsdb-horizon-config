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

import com.google.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class ConfigLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigLoader.class);

  public static <T> T loadResourceConfig(final String configFile, Class<T> type)
      throws IOException {
    final T t;

    try (final InputStream is = Resources.getResource(configFile).openStream()) {
      t = new Yaml().loadAs(is, type);
      LOGGER.info("Reading config from: " + Resources.getResource(configFile));
    }
    return t;
  }

  public static <T> T loadConfig(final String configFileUrl, Class<T> type) throws IOException {
    final T t;
    URL url = new URL(configFileUrl);
    try (final InputStream is = url.openStream()) {
      t = new Yaml().loadAs(is, type);
      LOGGER.info("Reading config from: " + configFileUrl);
    }
    return t;
  }
}
