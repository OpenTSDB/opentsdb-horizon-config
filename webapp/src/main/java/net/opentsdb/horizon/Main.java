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

import io.undertow.Undertow;
import net.opentsdb.horizon.config.Config;
import net.opentsdb.horizon.config.ConfigLoader;
import net.opentsdb.horizon.config.ServerConfig;
import net.opentsdb.horizon.server.UndertowServer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.Map;

import static net.opentsdb.horizon.ssl.KeyStoreSSLContextFactory.KEYSTORE_PATH;
import static net.opentsdb.horizon.util.Utils.isNullOrEmpty;

public class Main {

  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws Exception {

    Option configOption = new Option("c", "config.file", true, "url to the config file");

    final Options options = new Options();
    options.addOption(configOption);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    String configFile;
    String env = null;
    Config config;
    if (cmd.hasOption(configOption.getOpt())) {
      configFile = cmd.getOptionValue(configOption.getOpt());
      config = ConfigLoader.loadConfig(configFile, Config.class);
    } else {
      configFile = "config";
      env = args.length == 0 ? null : args[0];
      if (!isNullOrEmpty(env)) {
        env = env.split("-")[0].trim();
        configFile += "-" + env;
      }
      configFile += ".yaml";
      config = ConfigLoader.loadResourceConfig(configFile, Config.class);
    }

    final ServerConfig serverConfig = config.serverConfig;

    if (!isNullOrEmpty(env) && env.equalsIgnoreCase("dev")) {
      Map<String, Object> initParams = serverConfig.initParams;
      String jksKeyStorePath = (String) initParams.get(KEYSTORE_PATH);
      initParams.put(KEYSTORE_PATH, Paths.get(jksKeyStorePath).toAbsolutePath().toString());

      if (serverConfig.swaggerEnabled) {
        String swaggerHome = serverConfig.swaggerHome;
        if (swaggerHome.startsWith("~")) {
          String userHome = System.getProperty("user.home");
          serverConfig.swaggerHome = swaggerHome.replaceFirst("~", userHome);
        }
      }
    }

    ApplicationFactory applicationFactory = new ApplicationFactory(config);
    Undertow undertow = UndertowServer.getInstance(config, applicationFactory.buildApplication());
    undertow.start();
    LOGGER.info("Server listening on port: " + config.serverConfig.port);
  }
}
