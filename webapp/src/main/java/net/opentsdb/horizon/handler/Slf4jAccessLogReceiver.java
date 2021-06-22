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

package net.opentsdb.horizon.handler;

import io.undertow.server.handlers.accesslog.AccessLogReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Slf4jAccessLogReceiver implements AccessLogReceiver {

  public static final String FORMAT =  "%h %l %u \"%r\" %s %b %D \"%{i,Referer}\" \"%{i,User-Agent}\"";

  private final Logger logger;

  public Slf4jAccessLogReceiver(final String name) {
    logger = LoggerFactory.getLogger(name);
  }

  @Override
  public void logMessage(String message) {
    logger.info(message);
  }
}
