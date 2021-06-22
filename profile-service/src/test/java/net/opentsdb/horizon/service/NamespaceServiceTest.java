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

import net.opentsdb.horizon.fs.store.FolderStore;
import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.store.NamespaceStore;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import mockit.Tested;
import mockit.Verifications;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.aggregator.ArgumentsAccessor;
import org.junit.jupiter.params.provider.CsvSource;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.WebApplicationException;
import java.io.IOException;
import java.security.Principal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static net.opentsdb.horizon.service.NamespaceService.DH_TRACKS;
import static net.opentsdb.horizon.service.TestUtil.getMockedNamespace;
import static net.opentsdb.horizon.service.TestUtil.getNamespace;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class NamespaceServiceTest {

  private static final String TSDB_PRINCIPAL = "tsdb_principal";

  @Tested private NamespaceService namespaceService;

  @Injectable private NamespaceStore namespaceStore;

  @Injectable private FolderStore folderStore;

  @Injectable private NamespaceMemberService memberService;

  @Injectable private NamespaceFollowerService followerService;

  @Injectable private AuthService authService;

  @Mocked private Connection connection;

  @Mocked private HttpServletRequest httpServletRequest;

  @Test
  public void testGetAllNamespace() throws SQLException, IOException {
    Namespace namespace1 = getMockedNamespace();
    namespace1.setName("NS1");
    Namespace namespace2 = getMockedNamespace();
    namespace2.setName("NS2");

    final List<Namespace> mockedList = Arrays.asList(namespace1, namespace2);

    new Expectations(namespaceStore) {
      {
        namespaceStore.getAllNamespace(connection);
        result = mockedList;
      }
    };

    final List<Namespace> namespaceList = namespaceService.getAll();
    assertNotNull(namespaceList);
    assertEquals(namespaceList, mockedList);
  }

  @Test
  public void testGetNamespaceById() throws IOException, SQLException {
    Namespace mockNamespace = getMockedNamespace();

    new Expectations(namespaceStore) {
      {
        namespaceStore.getById(anyInt, connection);
        result = mockNamespace;
      }
    };

    final Namespace resultNamespace = namespaceService.getNamespace(5);
    assertNotNull(resultNamespace);
    assertEquals(mockNamespace, resultNamespace);
  }

  @Test
  public void testCreateNamespace() throws SQLException, IOException {
    Namespace namespace = getNamespace();

    new Expectations() {
      {
        namespaceStore.create(namespace, connection);
        result =
            new Delegate<Namespace>() {
              public int delegate() {
                namespace.setId(7);
                return 1;
              }
            };
      }
    };

    namespaceService.create(namespace, "authorizeduser");

    new Verifications() {
      {
        namespaceStore.create(namespace, connection);
        times = 1;

        namespaceStore.commit(connection);
        times = 1;
      }
    };

    assertEquals(7, namespace.getId().intValue());
    assertEquals("tsdbraw", namespace.getName());
    assertEquals("tsdbraw", namespace.getAlias());
    assertEquals(Arrays.asList("tsdbrawraw"), namespace.getMeta().get(DH_TRACKS));
    assertEquals(Boolean.TRUE, namespace.getEnabled());
    assertEquals("authorizeduser", namespace.getCreatedBy());
    assertEquals("authorizeduser", namespace.getUpdatedBy());
  }

  @Test
  public void shouldUseNamespaceNameAsAliasNameWhenNotProvided() throws SQLException, IOException {

    final Namespace namespace = getNamespace();
    namespace.setAlias(null);

    new Expectations() {
      {
        namespaceStore.create(namespace, connection);
        result =
            new Delegate<Namespace>() {
              public int delegate() {
                namespace.setId(7);
                return 1;
              }
            };
      }
    };

    final Namespace storedNamespace = namespaceService.create(namespace, "authorizeduser");

    new Verifications() {
      {
        namespaceStore.create(namespace, connection);
        times = 1;

        namespaceStore.commit(connection);
        times = 1;
      }
    };

    assertEquals(7, storedNamespace.getId().intValue());
    assertEquals("tsdbraw", storedNamespace.getName());
    assertEquals("tsdbraw", storedNamespace.getAlias());
    assertEquals(Arrays.asList("tsdbrawraw"), storedNamespace.getMeta().get(DH_TRACKS));
    assertEquals(Boolean.TRUE, storedNamespace.getEnabled());
    assertEquals("authorizeduser", storedNamespace.getCreatedBy());
    assertEquals("authorizeduser", storedNamespace.getUpdatedBy());
  }

  @ParameterizedTest
  @CsvSource({
    "Tsdb, Tsdb Alias, Tsdb, A New Alias", // namespace matched
    "Tsdb, Tsdb Alias, A New Tsdb, Tsdb Alias", // alias matched
    "Tsdb, Tsdb Alias, Tsdb Alias, A Different Alias", // alias to namespace matched
    "Tsdb, Tsdb Alias, A New Tsdb, Tsdb", // namespace to alias matched
    "Tsdb, Tsdb, Tsdb, Tsdb", // namespace and alias name matched
  })
  public void shouldNotCreateNamespaceWhenNameOrAliasAlreadyExist(ArgumentsAccessor arguments)
      throws SQLException, IOException {
    Namespace existingNamespace = new Namespace();
    existingNamespace.setName(arguments.getString(0));
    existingNamespace.setAlias(arguments.getString(1));
    existingNamespace.setMeta(new HashMap<>(){{put(DH_TRACKS, Arrays.asList("tsdbraw"));}});
    existingNamespace.setEnabled(true);

    new Expectations(namespaceStore) {
      {
        namespaceStore.getMatchedNamespace(anyString, anyString, withInstanceOf(Connection.class));
        result = Arrays.asList(existingNamespace);
      }
    };

    Namespace namespace = new Namespace();
    namespace.setName(arguments.getString(2));
    namespace.setAlias(arguments.getString(3));
    namespace.setMeta(new HashMap<>(){{put(DH_TRACKS, Arrays.asList("tsdbraw"));}});
    namespace.setEnabled(true);
    assertThrows(
        BadRequestException.class, () -> namespaceService.create(namespace, "authorizeduser"));
  }

  @Test
  public void shouldAddMemberWhenCreatingNamespace() throws SQLException, IOException {

    Namespace namespace = getNamespace();

    new Expectations() {
      {
        namespaceStore.getReadWriteConnection();
        result = connection;

        namespaceStore.create(namespace, connection);
        result =
            new Delegate<Namespace>() {
              public int delegate() {
                namespace.setId(7);
                return 1;
              }
            };
      }
    };

    namespaceService.create(namespace, "authorizeduser");

    new Verifications() {
      {
        namespaceStore.commit(connection);
        times = 1;

        List<String> capturedMembers;
        memberService.addNamespaceMemberWithoutCommit(
            7, capturedMembers = withCapture(), connection);
        times = 1;
        assertEquals(Arrays.asList("authorizeduser"), capturedMembers);
      }
    };
  }

  @Test
  public void createShouldRollbackOnSqlException(@Mocked String principal)
      throws SQLException, IOException {

    Namespace namespace = getNamespace();
    SQLException sqlException = new SQLException("Unable to obtain connection");

    new Expectations() {
      {
        namespaceStore.getReadWriteConnection();
        result = connection;

        namespaceStore.create(namespace, connection);
        result = sqlException;
      }
    };

    assertThrows(
        WebApplicationException.class, () -> namespaceService.create(namespace, principal));

    new Verifications() {
      {
        connection.commit();
        times = 0;

        namespaceStore.rollback(connection);
        times = 1;
      }
    };
  }

  @Test
  public void testNamespaceNameOrAliasShouldPresent() {
    Namespace namespace = getNamespace();
    assertThrows(
        BadRequestException.class,
        () -> namespaceService.update(null, null, namespace, httpServletRequest));
  }

  @Test
  public void testNamespaceNameOrAliasShouldNotEmpty() {
    Namespace namespace = getNamespace();
    assertThrows(
        BadRequestException.class,
        () -> namespaceService.update("", "", namespace, httpServletRequest));
  }

  @Test
  public void shouldEditNamespace() throws SQLException, IOException {
    Namespace namespace = getNamespace();
    final Namespace mockedNamespace = getMockedNamespace();

    Principal principal = () -> "authorizeduser";
    new Expectations() {
      {
        httpServletRequest.getUserPrincipal();
        result = principal;

        namespaceStore.getReadWriteConnection();
        result = connection;

        namespaceStore.getById(anyInt, connection);
        result = mockedNamespace;

        authService.authorize(mockedNamespace, principal.getName());
        result = true;
      }
    };

    namespaceService.update(mockedNamespace.getId(), namespace, httpServletRequest);

    new Verifications() {
      {
        Namespace captured;
        namespaceStore.update(captured = withCapture(), connection);
        times = 1;
        assertEquals(principal.getName(), captured.getUpdatedBy());

        namespaceStore.commit(connection);
        times = 1;
      }
    };
  }

  @ParameterizedTest
  @CsvSource({
    "Tsdb, Tsdb Alias, Tsdb, A New Alias", // namespace matched
    "Tsdb, Tsdb Alias, A New Tsdb, Tsdb Alias", // alias matched
    "Tsdb, Tsdb Alias, Tsdb Alias, A Different Alias", // alias to namespace matched
    "Tsdb, Tsdb Alias, A New Tsdb, Tsdb", // namespace to alias matched
    "Tsdb, Tsdb, Tsdb, Tsdb", // namespace and alias name matched
  })
  public void shouldNotAllowEditNamespaceWhenAliasNameAlreadyExist(
      String storedName, String storedAlias, String newname, String newalias)
      throws SQLException, IOException {
    Namespace existingNamespace = new Namespace();
    existingNamespace.setName(storedName);
    existingNamespace.setAlias(storedAlias);
    existingNamespace.setMeta(new HashMap<>(){{put(DH_TRACKS, Arrays.asList("tsdbraw"));}});
    existingNamespace.setEnabled(true);

    Principal principal = () -> "authorizeduser";

    new Expectations(namespaceStore) {
      {
        httpServletRequest.getUserPrincipal();
        result = principal;

        namespaceStore.getReadWriteConnection();
        result = connection;

        namespaceStore.getById(anyInt, connection);
        Namespace mockedNamespace = getMockedNamespace();
        result = mockedNamespace;

        namespaceStore.getMatchedNamespace(anyInt, anyString, anyString, connection);
        result = Arrays.asList(existingNamespace);

        authService.authorize(withInstanceOf(Namespace.class), principal.getName());
        result = true;
      }
    };

    Namespace namespace = new Namespace();
    namespace.setName(newname);
    namespace.setAlias(newalias);
    namespace.setMeta(new HashMap<>(){{put(DH_TRACKS, Arrays.asList("tsdbraw"));}});
    namespace.setEnabled(true);
    assertThrows(
        BadRequestException.class, () -> namespaceService.update(7, namespace, httpServletRequest));
  }

  @Test
  public void shouldPickUsernameCorrectlyWhenEditNamespace() throws SQLException, IOException {
    Namespace namespace = getNamespace();

    Principal principal = () -> "new-authorizeduser";
    new Expectations(namespaceStore) {
      {
        httpServletRequest.getUserPrincipal();
        result = principal;

        namespaceStore.getReadWriteConnection();
        result = connection;

        namespaceStore.getById(anyInt, connection);
        result = getMockedNamespace();

        authService.authorize(withInstanceOf(Namespace.class), principal.getName());
        result = true;
      }
    };

    namespaceService.update(7, namespace, httpServletRequest);

    new Verifications() {
      {
        Namespace captured;
        namespaceStore.update(captured = withCapture(), connection);
        assertEquals("new-authorizeduser", captured.getUpdatedBy());
        assertEquals(7, captured.getId().intValue());
      }
    };
  }

  @Test
  public void editShouldRollbackOnSqlException() throws SQLException, IOException {

    Namespace namespace = getNamespace();
    Principal principal = () -> "authorizeduser";
    new Expectations(namespaceStore) {
      {
        httpServletRequest.getUserPrincipal();
        result = principal;

        namespaceStore.getReadWriteConnection();
        result = connection;

        namespaceStore.getById(anyInt, connection);
        Namespace mockedNamespace = getMockedNamespace();
        result = mockedNamespace;

        authService.authorize(mockedNamespace, principal.getName());
        result = true;

        namespaceStore.update(mockedNamespace, connection);
        result = new SQLException("Unable to obtain connection");
      }
    };

    assertThrows(
        WebApplicationException.class,
        () -> namespaceService.update(7, namespace, httpServletRequest));

    new Verifications() {
      {
        connection.commit();
        times = 0;

        namespaceStore.rollback(connection);
        times = 1;
      }
    };
  }

  @Test
  public void shouldNotAllowNonMembersToEditNamespace() throws SQLException, IOException {
    Namespace namespace = getNamespace();
    Principal principal = () -> "authorizeduser";
    new Expectations() {
      {
        httpServletRequest.getUserPrincipal();
        result = principal;

        namespaceStore.getReadWriteConnection();
        result = connection;

        namespaceStore.getById(anyInt, connection);
        result = getMockedNamespace();

        authService.authorize(withInstanceOf(Namespace.class), principal.getName());
        result = false;
      }
    };
    assertThrows(
        ForbiddenException.class, () -> namespaceService.update(7, namespace, httpServletRequest));
  }
}
