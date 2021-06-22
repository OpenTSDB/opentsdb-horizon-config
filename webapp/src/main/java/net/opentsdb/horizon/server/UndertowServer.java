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

package net.opentsdb.horizon.server;

import io.ultrabrew.metrics.MetricRegistry;
import io.undertow.Undertow;
import io.undertow.Undertow.Builder;
import io.undertow.Undertow.ListenerBuilder;
import io.undertow.Undertow.ListenerType;
import io.undertow.UndertowOptions;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;
import io.undertow.server.handlers.accesslog.AccessLogHandler;
import io.undertow.server.handlers.resource.PathResourceManager;
import io.undertow.server.handlers.resource.ResourceHandler;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.FilterInfo;
import io.undertow.servlet.api.ServletInfo;
import net.opentsdb.horizon.config.Config;
import net.opentsdb.horizon.config.ServerConfig;
import net.opentsdb.horizon.ext.MetricRegistryFactory;
import net.opentsdb.horizon.filter.CorsFilter;
import net.opentsdb.horizon.handler.HealthCheckServletFactory;
import net.opentsdb.horizon.handler.Slf4jAccessLogReceiver;
import net.opentsdb.horizon.handler.UndertowMetricsHandler;
import net.opentsdb.horizon.ssl.SSLContextFactory;
import org.jboss.resteasy.core.ResteasyDeploymentImpl;
import org.jboss.resteasy.plugins.server.undertow.UndertowJaxrsServer;
import org.jboss.resteasy.spi.ResteasyDeployment;
import org.xnio.Options;
import org.xnio.Sequence;
import org.xnio.SslClientAuthMode;

import javax.net.ssl.SSLContext;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.http.HttpServlet;
import javax.ws.rs.core.Application;
import java.io.File;
import java.nio.file.Paths;
import java.util.List;

import static net.opentsdb.horizon.Utils.loadClass;
import static net.opentsdb.horizon.config.ApplicationConfig.ATHENZ_SSLCONTEXT;
import static net.opentsdb.horizon.filter.CorsFilter.ALLOWED_ORIGINS;
import static net.opentsdb.horizon.filter.CorsFilter.CORS_MAX_AGE;

public class UndertowServer {

  private static final String AUTH_FILTER = "authFilter";
  private static final String CORS_FILTER = "corsFilter";

  public static final String[] DEFAULT_CIPHER_SUITES =
      new String[] {
        "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
        "TLS_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_RSA_WITH_AES_128_GCM_SHA256",
        "TLS_RSA_WITH_AES_256_CBC_SHA256",
        "TLS_RSA_WITH_AES_128_CBC_SHA256",
        "TLS_RSA_WITH_AES_256_CBC_SHA",
        "TLS_RSA_WITH_AES_128_CBC_SHA"
      };

  public static Undertow getInstance(final Config config, Application application)
      throws Exception {

    ServerConfig serverConfig = config.serverConfig;

    int port = serverConfig.port;
    Builder builder = Undertow.builder();
    ListenerBuilder listenerBuilder = new ListenerBuilder().setHost("0.0.0.0").setPort(port);

    if (serverConfig.sslContextFactoryClass == null) {
      listenerBuilder.setType(ListenerType.HTTP);
    } else {
      SSLContext athenzSSLContext =
          (SSLContext) config.applicationConfig.initParams.get(ATHENZ_SSLCONTEXT);
      SSLContext sslContext = buildSSLContext(serverConfig, athenzSSLContext);
      listenerBuilder.setType(ListenerType.HTTPS).setSslContext(sslContext);
      String[] ciphers = serverConfig.ciphers;
      if (ciphers == null) {
        ciphers = DEFAULT_CIPHER_SUITES;
      }
      Sequence<String> cipherSuites = Sequence.of(ciphers);
      builder
          .setSocketOption(Options.SSL_CLIENT_AUTH_MODE, SslClientAuthMode.REQUESTED)
          .setSocketOption(Options.SSL_ENABLED_CIPHER_SUITES, cipherSuites)
          .setServerOption(UndertowOptions.SSL_USER_CIPHER_SUITES_ORDER, true);
    }

    builder
        .addListener(listenerBuilder)
        .setSocketOption(Options.BACKLOG, 150)
        .setBufferSize(1024 * 16)
        .setIoThreads(Math.max(1, Runtime.getRuntime().availableProcessors() - 1))
        .setServerOption(UndertowOptions.ALWAYS_SET_DATE, true)
        .setWorkerOption(Options.CONNECTION_HIGH_WATER, 7000)
        .setWorkerOption(Options.CONNECTION_LOW_WATER, 5000)
        .setWorkerThreads(100);

    String builderCustomizerClassName = serverConfig.builderCustomizerClassName;
    if (null != builderCustomizerClassName && !builderCustomizerClassName.isEmpty()) {
      UndertowBuilderCustomizer ubc =
          ((Class<? extends UndertowBuilderCustomizer>) loadClass(builderCustomizerClassName))
              .newInstance();
      ubc.customize(builder);
    }

    HttpHandler root = createServer(config, application);
    return builder.setHandler(root).build();
  }

  private static HttpHandler createServer(Config config, Application application) throws Exception {
    ResteasyDeployment deployment = new ResteasyDeploymentImpl();
    deployment.setApplication(application);

    UndertowJaxrsServer server = new UndertowJaxrsServer();
    DeploymentInfo deploymentInfo =
        server
            .undertowDeployment(deployment, "/")
            .setClassLoader(UndertowServer.class.getClassLoader())
            .setContextPath("/api")
            .setDeploymentName("API");

    ServerConfig serverConfig = config.serverConfig;

    if (serverConfig.corsEnabled) {
      FilterInfo corsFilter = createCorsFilter(serverConfig);
      deploymentInfo
          .addFilter(corsFilter)
          .addFilterUrlMapping(CORS_FILTER, "/*", DispatcherType.REQUEST);
    }

    FilterInfo authFilter = createAuthFilter(serverConfig);
    deploymentInfo
        .addFilter(authFilter)
        .addFilterUrlMapping(AUTH_FILTER, "/*", DispatcherType.REQUEST);

    DeploymentManager deploymentManager = Servlets.defaultContainer().addDeployment(deploymentInfo);
    deploymentManager.deploy();
    HttpHandler apiHandler = deploymentManager.start();

    final PathHandler pathHandler = new PathHandler();
    pathHandler.addPrefixPath(deploymentInfo.getContextPath(), apiHandler);

    String healthCheckServletFactoryClassName = serverConfig.healthCheckServletFactoryClassName;
    if (healthCheckServletFactoryClassName != null
        && !healthCheckServletFactoryClassName.isEmpty()) {
      HealthCheckServletFactory factory =
          ((Class<? extends HealthCheckServletFactory>)
                  loadClass(healthCheckServletFactoryClassName))
              .getDeclaredConstructor()
              .newInstance();

      HttpHandler healthCheckHandler =
          getDeployment(
                  "healthCheck",
                  "/",
                  factory.createServlet(),
                  "healthServlet",
                  factory.getMappings())
              .start();
      pathHandler.addPrefixPath("/", healthCheckHandler);
    }

    if (serverConfig.swaggerEnabled) {
      ResourceHandler resourceHandler =
          new ResourceHandler(
                  new PathResourceManager(
                      Paths.get(new File(serverConfig.swaggerHome).getAbsolutePath()), 100))
              .setDirectoryListingEnabled(true)
              .addWelcomeFiles("index.html");
      pathHandler.addPrefixPath("/swagger", resourceHandler);
    }

    HttpHandler rootHandler = pathHandler;

    if (serverConfig.instrumentationEnabled) {
      String metricRegistryFactoryClassName = serverConfig.metricRegistryFactoryClassName;
      MetricRegistryFactory factory =
          ((Class<? extends MetricRegistryFactory>) loadClass(metricRegistryFactoryClassName))
              .getDeclaredConstructor()
              .newInstance();
      MetricRegistry metricRegistry = factory.createRegistry(serverConfig.initParams);
      rootHandler = new UndertowMetricsHandler(metricRegistry, rootHandler);
    }

    if (serverConfig.accesslogEnabled) {
      rootHandler =
          new AccessLogHandler(
              rootHandler,
              new Slf4jAccessLogReceiver(serverConfig.accesslogLoggerName),
              serverConfig.accesslogFormat,
              UndertowServer.class.getClassLoader());
    }

    return rootHandler;
  }

  private static DeploymentManager getDeployment(
      final String deploymentName,
      final String contextPath,
      final HttpServlet httpServlet,
      final String servletName,
      final List<String> mappings) {
    final ServletInfo servlet =
        Servlets.servlet(servletName, httpServlet.getClass()).addMappings(mappings);

    final DeploymentInfo servletBuilder =
        Servlets.deployment()
            .setClassLoader(UndertowServer.class.getClassLoader())
            .setDeploymentName(deploymentName)
            .setContextPath(contextPath)
            .addServlets(servlet);

    final DeploymentManager manager = Servlets.defaultContainer().addDeployment(servletBuilder);
    manager.deploy();
    return manager;
  }

  private static FilterInfo createCorsFilter(ServerConfig serverConfig) {
    FilterInfo corsFilter = new FilterInfo(CORS_FILTER, CorsFilter.class);
    corsFilter.addInitParam(ALLOWED_ORIGINS, serverConfig.allowedOrigin);
    corsFilter.addInitParam(CORS_MAX_AGE, String.valueOf(serverConfig.corsMaxAge));
    return corsFilter;
  }

  private static FilterInfo createAuthFilter(ServerConfig serverConfig) {
    String authFilterClassName = serverConfig.authFilterClassName;
    Class<? extends Filter> authFilterClass =
        (Class<? extends Filter>) loadClass(authFilterClassName);
    FilterInfo authFilter = new FilterInfo(AUTH_FILTER, authFilterClass);
    serverConfig
        .initParams
        .entrySet()
        .forEach(entry -> authFilter.addInitParam(entry.getKey(), entry.getValue().toString()));
    return authFilter;
  }

  private static SSLContext buildSSLContext(ServerConfig config, SSLContext athenzSSLContext)
      throws Exception {
    config.initParams.put(ATHENZ_SSLCONTEXT, athenzSSLContext);
    SSLContextFactory sslContextFactory =
        ((Class<? extends SSLContextFactory>) loadClass(config.sslContextFactoryClass))
            .newInstance();
    SSLContext sslContext = sslContextFactory.createSSLContext(config.initParams);
    config.initParams.remove(ATHENZ_SSLCONTEXT);
    return sslContext;
  }
}
