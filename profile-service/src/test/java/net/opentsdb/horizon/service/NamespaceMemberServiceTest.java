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
import net.opentsdb.horizon.UserCache;
import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.model.User;
import net.opentsdb.horizon.store.NamespaceFollowerStore;
import net.opentsdb.horizon.store.NamespaceMemberStore;
import com.yahoo.athenz.zms.ZMSClient;
import com.yahoo.athenz.zts.ZTSClient;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import mockit.Tested;
import mockit.Verifications;
import org.junit.jupiter.api.Test;

import javax.ws.rs.ForbiddenException;
import javax.ws.rs.WebApplicationException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class NamespaceMemberServiceTest {

  @Tested private NamespaceMemberService service;
  @Injectable private NamespaceMemberStore memberStore;
  @Injectable private NamespaceFollowerStore followerStore;
  @Injectable private AuthService authService;
  @Injectable private ZTSClient ztsClient;
  @Injectable private ZMSClient zmsClient;
  @Injectable private NamespaceCache namespaceCache;
  @Injectable private UserCache userCache;
  @Injectable private String providerDomain;
  @Injectable private String providerService;

  @Mocked private Connection connection;

  @Test
  public void testGetNamespaceMember() throws Exception {
    User mockedUser = new User();
    mockedUser.setName("test-mockedUser");
    mockedUser.setUserid("test-mockedUser");
    mockedUser.setEnabled(true);

    final Namespace namespace = new Namespace();
    namespace.setMeta(new HashMap<>());
    new Expectations() {
      {
        memberStore.getReadOnlyConnection();
        result = connection;
        memberStore.getNamespaceMembers(anyInt, connection);
        result = Arrays.asList(mockedUser);
        namespaceCache.getById(anyInt);
        result = namespace;
      }
    };

    final List<User> namespaceMembers = service.getNamespaceMember(7);
    assertEquals(1, namespaceMembers.size());
    final User member = namespaceMembers.get(0);
    assertEquals(member.getName(), mockedUser.getName());
    assertEquals(member.getUserid(), mockedUser.getUserid());
    assertEquals(member.isEnabled(), mockedUser.isEnabled());
  }

  @Test
  public void testAddNamespaceMember() throws Exception {

    Namespace namespace = new Namespace();
    namespace.setMeta(new HashMap<>());
    new Expectations() {
      {
        namespaceCache.getById(anyInt);
        result = namespace;
        authService.authorize(withInstanceOf(Namespace.class), anyString);
        result = true;
      }
    };

    final List<String> memberIdList = Arrays.asList("user.name1", "user.name2");
    service.addNamespaceMember(6, memberIdList, "authorizeduser");

    new Verifications() {
      {
        List<String> capturedMembers;
        memberStore.addMembers(6, capturedMembers = withCapture(), connection);
        times = 1;
        assertEquals(memberIdList, capturedMembers);

        List<String> capturedFollowers;
        followerStore.removeFollowers(6, capturedFollowers = withCapture(), connection);
        times = 1;
        assertEquals(memberIdList, capturedFollowers);

        memberStore.commit(connection);
        times = 1;
      }
    };
  }

  @Test
  public void testRemoveNamespaceMember() throws SQLException {

    new Expectations() {
      {
        authService.authorize(withInstanceOf(Namespace.class), anyString);
        result = true;
      }
    };

    final List<String> memberIdList = Arrays.asList("user.name1", "user.name2");
    service.removeNamespaceMember(6, memberIdList, "authorizeduser");

    new Verifications() {
      {
        List<String> capturedMembers;
        memberStore.removeNamespaceMember(6, capturedMembers = withCapture(), connection);
        times = 1;
        assertEquals(memberIdList, capturedMembers);

        memberStore.commit(connection);
        times = 1;
      }
    };
  }

  @Test
  public void shouldNotAllowNonMemberToAddOtherMember() throws Exception {

    Namespace namespace = new Namespace();
    namespace.setMeta(new HashMap<>());

    new Expectations() {
      {
        authService.authorize(withInstanceOf(Namespace.class), anyString);
        result = false;
        namespaceCache.getById(anyInt);
        result = namespace;
      }
    };

    final List<String> memberIdList = Arrays.asList("user.name1", "user.name2");
    assertThrows(
        ForbiddenException.class,
        () -> service.addNamespaceMember(6, memberIdList, "authorizeduser"));

    new Verifications() {
      {
        memberStore.addMembers(
            anyInt, withInstanceOf(List.class), withInstanceOf(Connection.class));
        times = 0;
        followerStore.removeFollowers(
            anyInt, withInstanceOf(List.class), withInstanceOf(Connection.class));
        times = 0;
        connection.commit();
        times = 0;
      }
    };
  }

  @Test
  public void shouldNotAllowNonMemberToRemoveOtherMember() throws SQLException {

    new Expectations() {
      {
        authService.authorize(withInstanceOf(Namespace.class), anyString);
        result = false;
      }
    };

    final List<String> memberIdList = Arrays.asList("user.name1", "user.name2");
    assertThrows(
        ForbiddenException.class,
        () -> service.removeNamespaceMember(6, memberIdList, "authorizeduser"));

    new Verifications() {
      {
        memberStore.removeNamespaceMember(
            anyInt, withInstanceOf(List.class), withInstanceOf(Connection.class));
        times = 0;
        connection.commit();
        times = 0;
      }
    };
  }

  @Test
  public void shouldRollbackWhenAddMembersThrowsSQLException() throws Exception {

    Namespace namespace = new Namespace();
    namespace.setMeta(new HashMap<>());
    new Expectations() {
      {
        namespaceCache.getById(anyInt);
        result = namespace;
        authService.authorize(withInstanceOf(Namespace.class), anyString);
        result = true;

        memberStore.addMembers(anyInt, withInstanceOf(List.class), connection);
        result = new SQLException("Oops...");
      }
    };

    List<String> memberIdList = Arrays.asList("user.name1", "user.name2");
    assertThrows(
        WebApplicationException.class,
        () -> service.addNamespaceMember(6, memberIdList, "authorizeduser"));

    new Verifications() {
      {
        memberStore.addMembers(6, memberIdList, connection);
        times = 1;
        followerStore.removeFollowers(6, memberIdList, connection);
        times = 0;
        memberStore.rollback(connection);
        times = 1;
        connection.commit();
        times = 0;
      }
    };
  }

  @Test
  public void shouldRollbackWhenRemoveFollowerThrowsSQLException() throws SQLException {

    new Expectations() {
      {
        authService.authorize(withInstanceOf(Namespace.class), anyString);
        result = true;

        memberStore.getReadWriteConnection();
        result = connection;
        memberStore.removeNamespaceMember(anyInt, withInstanceOf(List.class), connection);
        result = new SQLException("Oops...");
      }
    };

    List<String> memberIdList = Arrays.asList("user.name1", "user.name2");
    assertThrows(
        WebApplicationException.class,
        () -> service.removeNamespaceMember(6, memberIdList, "authorizeduser"));

    new Verifications() {
      {
        memberStore.removeNamespaceMember(6, memberIdList, connection);
        times = 1;
        memberStore.rollback(connection);
        times = 1;
        connection.commit();
        times = 0;
      }
    };
  }
}
