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

package net.opentsdb.horizon.service;

import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.model.User;
import net.opentsdb.horizon.store.NamespaceMemberStore;
import com.yahoo.athenz.zts.ZTSClient;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import mockit.Tested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class AuthServiceTest {

  @Tested private AuthService authService;

  @Injectable private NamespaceMemberStore namespaceMemberStore;
  @Injectable private ZTSClient ztsClient;
  @Mocked private Connection connection;
  @Injectable private String athensDomain;
  @Injectable private String providerDomain;
  @Injectable private String providerService;

  @ParameterizedTest
  @MethodSource("testAuthorization")
  public void testAuthorization(
      String username1, String username2, boolean result, @Mocked Connection connection)
      throws SQLException {

    User mockedUser = new User();
    mockedUser.setName(username1);
    mockedUser.setUserid(username1);
    mockedUser.setEnabled(true);

    Namespace namespace = new Namespace();
    namespace.setMeta(new HashMap<>());
    namespace.setId(6);

    new Expectations(namespaceMemberStore) {
      {
        namespaceMemberStore.getNamespaceMembers(6, withInstanceOf(Connection.class));
        result = Arrays.asList(mockedUser);
      }
    };

    assertEquals(result, authService.authorize(namespace, username2));
  }

  private static Stream<Arguments> testAuthorization() {
    return Stream.of(
        arguments("username-1", "username-1", true), arguments("username-1", "username-2", false));
  }
}
