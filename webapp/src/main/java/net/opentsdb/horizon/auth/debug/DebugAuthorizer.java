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

package net.opentsdb.horizon.auth.debug;

import net.opentsdb.horizon.auth.Authorizer;

/**
 * A dummy implementation of {@link Authorizer}. Easy to fake access grant for arbitrary user.
 * Should only be used for testing and NOT IN PRODUCTION.
 */
public class DebugAuthorizer implements Authorizer<DebugPrincipal> {
  @Override
  public boolean checkAccess(String action, String resource, DebugPrincipal principal) {
    return principal.hasAccess();
  }
}
