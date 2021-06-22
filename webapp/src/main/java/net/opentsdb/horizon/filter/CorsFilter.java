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

package net.opentsdb.horizon.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public class CorsFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(CorsFilter.class);

    public static final String OPTIONS_METHOD = "OPTIONS";

    public static final String ORIGIN_HEADER = "Origin";
    public static final String ACCESS_CONTROL_REQUEST_METHOD_HEADER = "Access-Control-Request-Method";
    public static final String ACCESS_CONTROL_REQUEST_HEADERS_HEADER = "Access-Control-Request-Headers";

    public static final String ACCESS_CONTROL_ALLOW_ORIGIN_HEADER = "Access-Control-Allow-Origin";
    public static final String ACCESS_CONTROL_ALLOW_METHODS_HEADER = "Access-Control-Allow-Methods";
    public static final String ACCESS_CONTROL_ALLOW_HEADERS_HEADER = "Access-Control-Allow-Headers";
    public static final String ACCESS_CONTROL_MAX_AGE_HEADER = "Access-Control-Max-Age";
    public static final String ACCESS_CONTROL_ALLOW_CREDENTIALS_HEADER = "Access-Control-Allow-Credentials";

    public static final String ALLOWED_ORIGINS = "allowedOrigins";
    public static final String ALLOWED_METHODS = "allowedMethods";
    public static final String ALLOWED_HEADERS = "allowedHeaders";
    public static final String ALLOW_CREDENTIALS = "allowCredentials";
    public static final String CORS_MAX_AGE = "corsMaxAge";

    private static final String ALL_ORIGINS = "*";
    private static final String ALL_METHODS = "GET, DELETE, OPTIONS, PATCH, POST, PUT";
    private static final String DEFAULT_ALLOWED_HEADERS = "Accept, Content-Type, Origin, X-Requested-With";
    private static final String DEFAULT_MAX_AGE = "1800";


    private Set<String> allowedOrigins;
    private String allowedMethods;
    private String allowedHeaders;
    private String maxAge;

    private boolean allowAllOrigin;
    private boolean allowAnyHeader;
    private boolean allowCredentials = true;
    private Map<String, Pattern> originPatternMap;
    private Set<String> allowedMethodSet;
    private Set<String> allowedHeaderSet;

    @Override
    public void init(FilterConfig filterConfig) {

        this.allowedOrigins = parseOrigin(filterConfig.getInitParameter(ALLOWED_ORIGINS));
        this.allowedMethods = filterConfig.getInitParameter(ALLOWED_METHODS);
        this.allowedHeaders = filterConfig.getInitParameter(ALLOWED_HEADERS);
        this.allowAllOrigin = allowedOrigins.contains(ALL_ORIGINS);
        this.maxAge = filterConfig.getInitParameter(CORS_MAX_AGE);

        String allowCredentialsConfig = filterConfig.getInitParameter(ALLOW_CREDENTIALS);

        if (allowCredentialsConfig != null && !allowCredentialsConfig.isEmpty()) {
            this.allowCredentials = Boolean.parseBoolean(allowCredentialsConfig);
        }
        if (allowedMethods == null || allowedMethods.isEmpty()) {
            allowedMethods = ALL_METHODS;
        }
        if (allowedHeaders == null || allowedHeaders.isEmpty()) {
            allowedHeaders = DEFAULT_ALLOWED_HEADERS;
        }
        if (allowedHeaders.equals("*")) {
            allowAnyHeader = true;
        }
        if (maxAge == null || maxAge.isEmpty()) {
            this.maxAge = DEFAULT_MAX_AGE;
        }

        this.originPatternMap = allowedOrigins.stream().collect(toMap(Function.identity(), origin -> createPattern(origin)));
        this.allowedMethodSet = Arrays.stream(allowedMethods.split(",")).map(method -> method.trim()).collect(Collectors.toSet());
        this.allowedHeaderSet = Arrays.stream(allowedHeaders.split(",")).map(header -> header.trim().toLowerCase()).collect(Collectors.toSet());
    }

    private Pattern createPattern(String pattern) {
        String regex = pattern.replace(".", "\\.");
        regex = regex.replace("*", ".*"); // we want to be greedy here to match multiple subdomains, thus we use .*
        return Pattern.compile(regex);
    }

    private Set<String> parseOrigin(String allowedOriginsConfig) {
        if(allowedOriginsConfig == null || allowedOriginsConfig.isEmpty()){
            allowedOriginsConfig = ALL_ORIGINS;
        }
        String[] allowedOriginArray = allowedOriginsConfig.split(",");
        return Arrays.stream(allowedOriginArray).map(origin -> origin.trim()).collect(toSet());
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        handle((HttpServletRequest)request, (HttpServletResponse)response, chain);
    }

    private void handle(HttpServletRequest request, HttpServletResponse response, FilterChain chain) throws IOException, ServletException {
        String origin = request.getHeader(ORIGIN_HEADER);
        if (origin != null) // a cross origin request
        {
            if (allowAllOrigin || originMatches(origin)) {
                if (isPreflightRequest(request)) {
                    logger.debug("Handling the preflight request: {}", request.getRequestURI());
                    handlePreflightRequest(request, response, origin);
                    return;
                } else {
                    logger.debug("Handling the simple request: {}", request.getRequestURI());
                    handleSimpleResponse(request, response, origin);
                }
            }
        }
        chain.doFilter(request, response);
    }

    private void handleSimpleResponse(HttpServletRequest request, HttpServletResponse response, String origin) {
        response.setHeader(ACCESS_CONTROL_ALLOW_ORIGIN_HEADER, origin);
        if (allowCredentials) {
            response.setHeader(ACCESS_CONTROL_ALLOW_CREDENTIALS_HEADER, "true");
        }
    }

    private void handlePreflightRequest(HttpServletRequest request, HttpServletResponse response, String origin){
        String requestMethod = request.getHeader(ACCESS_CONTROL_REQUEST_METHOD_HEADER);
        if (!isMethodAllowed(requestMethod)) {
            logger.debug("Method {} not allowed. Allowed methods: {}", requestMethod, allowedMethods);
            return;
        }
        String requestHeaders = request.getHeader(ACCESS_CONTROL_REQUEST_HEADERS_HEADER);
        if (!allowAnyHeader && !areHeadersAllowed(requestHeaders)) {
            logger.debug("Headers {} not allowed. Allowed headers: {}", requestHeaders, allowedHeaders);
            return;
        }

        response.setHeader(ACCESS_CONTROL_ALLOW_ORIGIN_HEADER, origin);
        response.setHeader(ACCESS_CONTROL_ALLOW_METHODS_HEADER, allowedMethods);
        response.setHeader(ACCESS_CONTROL_MAX_AGE_HEADER, maxAge);
        if (allowCredentials) {
            response.setHeader(ACCESS_CONTROL_ALLOW_CREDENTIALS_HEADER, "true");
        }
        if (allowAnyHeader) {
            response.setHeader(ACCESS_CONTROL_ALLOW_HEADERS_HEADER, requestHeaders);
        }else{
            response.setHeader(ACCESS_CONTROL_ALLOW_HEADERS_HEADER, allowedHeaders);
        }
    }

    private boolean areHeadersAllowed(String requestHeaders) {
        if (requestHeaders == null) {
            return true;
        }
        Set<String> requestHeaderSet = Arrays.stream(requestHeaders.split(",")).map(origin -> origin.trim().toLowerCase()).collect(Collectors.toSet());
        return allowedHeaderSet.containsAll(requestHeaderSet);
    }

    private boolean isMethodAllowed(String requestMethod) {
        return allowedMethodSet.contains(requestMethod);
    }

    private boolean isPreflightRequest(HttpServletRequest request) {
        return OPTIONS_METHOD.equalsIgnoreCase(request.getMethod())
            && request.getHeader(ACCESS_CONTROL_REQUEST_METHOD_HEADER) != null;
    }

    private boolean originMatches(String origin) {

        for(Map.Entry<String, Pattern> entry: originPatternMap.entrySet()){
            String allowedOrigin = entry.getKey();
            if(allowedOrigin.contains("*")){
                Pattern pattern = entry.getValue();
                Matcher matcher = pattern.matcher(origin);
                if (matcher.matches()) {
                    return true;
                }
            }
            else {
                if(allowedOrigin.equals(origin)){
                    return true;
                }
            }
        }
        logger.debug("Origin not allowed {}", origin);
        return false;
    }

    @Override
    public void destroy() {
    }
}
