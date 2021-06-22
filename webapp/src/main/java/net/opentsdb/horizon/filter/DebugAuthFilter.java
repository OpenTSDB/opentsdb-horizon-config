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



import net.opentsdb.horizon.auth.Authority;
import net.opentsdb.horizon.auth.debug.DebugAuthority;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.Principal;

public class DebugAuthFilter implements Filter {

  public static final String COOKIE_NAME = "auth.cookie.name";

  private String cookieName;

  private Authority authority = new DebugAuthority();

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    String cookieName = filterConfig.getInitParameter(COOKIE_NAME);
    if (cookieName == null || cookieName.isEmpty()) {
      throw new ServletException(COOKIE_NAME + " is missing from filter configuration");
    }
    this.cookieName = cookieName;
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    final HttpServletRequest httpServletRequest = (HttpServletRequest) request;
    final HttpServletResponse httpServletResponse = (HttpServletResponse) response;
    Cookie cookie = getCookie(httpServletRequest, cookieName);
    if (null == cookie) {
      httpServletResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED, "auth cookie not found");
    } else {
      Principal principal = authority.authenticate(cookie.getValue());
      if (principal == null) {
        httpServletResponse.sendError(
            HttpServletResponse.SC_UNAUTHORIZED, "authentication failure");
      } else {
        HttpServletRequestWrapper requestWrapper =
            new HttpServletRequestWrapper(httpServletRequest) {
              @Override
              public Principal getUserPrincipal() {
                return principal;
              }
            };
        chain.doFilter(requestWrapper, response);
      }
    }
  }

  private Cookie getCookie(HttpServletRequest request, String cookieName) {
    Cookie[] cookies = request.getCookies();
    if (cookies != null) {
      for (Cookie cookie : cookies) {
        if (cookieName.equals(cookie.getName())) {
          return cookie;
        }
      }
    }
    return null;
  }
}
