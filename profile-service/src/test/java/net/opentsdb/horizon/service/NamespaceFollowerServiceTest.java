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

import net.opentsdb.horizon.NamespaceCache;
import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.model.User;
import net.opentsdb.horizon.store.NamespaceFollowerStore;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import mockit.Tested;
import mockit.Verifications;
import org.junit.jupiter.api.Test;

import javax.ws.rs.BadRequestException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class NamespaceFollowerServiceTest {

  @Tested private NamespaceFollowerService followerService;
  @Injectable private NamespaceFollowerStore followerStore;
  @Injectable private AuthService authService;
  @Injectable private NamespaceCache namespaceCache;

  @Mocked private Connection connection;

  @Test
  public void testGetNamespaceFollowers() throws SQLException {
    User mockedUser = new User();
    mockedUser.setName("test-mockedUser");
    mockedUser.setUserid("test-mockedUser");
    mockedUser.setEnabled(true);

    new Expectations() {
      {
        followerStore.getReadOnlyConnection();
        result = connection;
        followerStore.getNamespaceFollowers(anyInt, connection);
        result = Arrays.asList(mockedUser);
      }
    };

    final List<User> namespaceMembers = followerService.getNamespaceFollowers(7);
    assertEquals(1, namespaceMembers.size());
    final User member = namespaceMembers.get(0);
    assertEquals(member.getName(), mockedUser.getName());
    assertEquals(member.getUserid(), mockedUser.getUserid());
    assertEquals(member.isEnabled(), mockedUser.isEnabled());
  }

  @Test
  public void testAddNamespaceFollowers() throws SQLException {

    new Expectations() {
      {
        followerStore.getReadWriteConnection();
        result = connection;
        authService.authorize(withInstanceOf(Namespace.class), anyString);
        result = false;
      }
    };

    final String principal = "authorizeduser";
    followerService.addNamespaceFollower(6, principal);

    new Verifications() {
      {
        List<String> capturedFollowers;
        followerStore.addFollowers(6, capturedFollowers = withCapture(), connection);
        times = 1;
        assertEquals(Arrays.asList(principal), capturedFollowers);

        followerStore.commit(connection);
        times = 1;
      }
    };
  }

  @Test
  public void testRemoveNamespaceFollowers() throws SQLException {

    new Expectations() {
      {
        followerStore.getReadWriteConnection();
        result = connection;
      }
    };

    final String principal = "authorizeduser";
    followerService.removeNamespaceFollower(6, principal);

    new Verifications() {
      {
        List<String> capturedFollowers;
        followerStore.removeFollowers(6, capturedFollowers = withCapture(), connection);
        times = 1;
        assertEquals(Arrays.asList(principal), capturedFollowers);

        followerStore.commit(connection);
        times = 1;
      }
    };
  }

  @Test
  public void testFollowerShouldNotBeMember() throws SQLException {

    new Expectations() {
      {
        authService.authorize(withInstanceOf(Namespace.class), anyString);
        result = true;
      }
    };

    assertThrows(
        BadRequestException.class,
        () -> followerService.addNamespaceFollower(6, "un-authorizeduser"));
  }
}
