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

import io.ultrabrew.metrics.MetricRegistry;
import io.ultrabrew.metrics.Timer;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;

public class UndertowMetricsHandler implements HttpHandler {

  private static final String HTTP_REQUEST_METRIC = "http.request";
  private static final String DIMENSION_HTTP_METHOD = "method";
  private static final String DIMENSION_HTTP_STATUS = "status";

  private final Timer requestTimer;
  private HttpHandler next;

  public UndertowMetricsHandler(final MetricRegistry metricRegistry, final HttpHandler next) {
    this(metricRegistry, next, HTTP_REQUEST_METRIC);
  }

  public UndertowMetricsHandler(
      final MetricRegistry metricRegistry, final HttpHandler next, final String metricId) {
    this.requestTimer = metricRegistry.timer(metricId);
    this.next = next;
  }

  @Override
  public void handleRequest(HttpServerExchange exchange) throws Exception {

    long start = requestTimer.start();
    exchange.addExchangeCompleteListener(
        (ex, nxt) -> {
          requestTimer.stop(
              start,
              DIMENSION_HTTP_METHOD,
              ex.getRequestMethod().toString(),
              DIMENSION_HTTP_STATUS,
              Integer.toString(ex.getStatusCode()));
          nxt.proceed();
        });
    next.handleRequest(exchange);
  }
}
