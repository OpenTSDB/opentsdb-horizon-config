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

import com.google.common.base.Strings;
import net.opentsdb.horizon.converter.NamespaceConverter;
import net.opentsdb.horizon.fs.Path;
import net.opentsdb.horizon.fs.Path.PathException;
import net.opentsdb.horizon.fs.model.Folder;
import net.opentsdb.horizon.fs.store.FolderStore;
import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.profile.Utils;
import net.opentsdb.horizon.store.NamespaceStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static net.opentsdb.horizon.util.Utils.isNullOrEmpty;
import static net.opentsdb.horizon.util.Utils.slugify;

public class NamespaceService extends ProfileService<Namespace, Namespace, NamespaceConverter> {

  private static final Logger LOGGER = LoggerFactory.getLogger(NamespaceService.class);

  public static final String SO_SERVICE = "HZ_NAMESPACE_SERVICE";

  public static final String GIT_CONFIG_URL = "gitConfigUrl";
  public static final String JIRA_PROJECT_NAME = "jiraProjectName";
  public static final String EMAILID = "emailId";
  public static final String ATHENS_DOMAIN = "athensDomain";
  public static final String DH_TRACKS = "dhTracks";

  private final NamespaceStore store;
  private final NamespaceFollowerService followerService;
  private final NamespaceMemberService memberService;
  private FolderStore folderStore;
  private final AuthService authService;

  public NamespaceService(
      final NamespaceStore store,
      final NamespaceMemberService memberService,
      final NamespaceFollowerService followerService,
      final FolderStore folderStore,
      final AuthService authService) {

    super(new NamespaceConverter(), store);

    this.store = store;
    this.memberService = memberService;
    this.followerService = followerService;
    this.folderStore = folderStore;
    this.authService = authService;
  }

  /**
   * Fetch namespace by namespace or alias name
   *
   * @return namespace. If not available return null
   */
  public Namespace getNamespace(
      String name, String alias, boolean includeNameOrAlias) {
    final Namespace namespace =
        (name != null)
            ? getNamespaceByName(name, includeNameOrAlias)
            : getNamespaceByAlias(alias, includeNameOrAlias);
    return namespace;
  }

  /**
   * Fetch all namespace
   *
   * @return List of namespace
   */
  public List<Namespace> getAll() {
    final String message = "Error listing all namespaces";
    return list((connection) -> store.getAllNamespace(connection), message);
  }

  /**
   * Fetch namespace by namespace name
   *
   * @return namespace. If not available return null
   */
  public Namespace getNamespaceByName(
      String name, boolean includeNameOrAlias) {

    String format = "Error reading namespace by name: %s";
    Namespace namespace =
        get(
            (connection) ->
                includeNameOrAlias
                    ? store.getNamespaceByNameOrAlias(name, connection)
                    : store.getNamespaceByName(name, connection),
            format,
            name);
    if (namespace == null) {
      throw notFoundException("Namespace not found with name: " + name);
    }
    return namespace;
  }

  /**
   * Fetch namespace by namespace alias name
   *
   * @return namespace. If not available return null
   */
  public Namespace getNamespaceByAlias(
      String alias, boolean includeNameOrAlias) {
    String format = "Error reading namespace by alias: %s";
    Namespace namespace =
        get(
            (connection) ->
                includeNameOrAlias
                    ? store.getNamespaceByNameOrAlias(alias, connection)
                    : store.getNamespaceByAlias(alias, connection),
            format,
            alias);
    if (namespace == null) {
      throw notFoundException("Namespace not found with alias: " + alias);
    }
    return namespace;
  }

  /**
   * Fetch namespace by namespace id
   *
   * @param namespaceId
   * @return namespace. If not available return null
   */
  public Namespace getNamespace(int namespaceId) {
    String format = "Error reading namespace by id: %d";
    Namespace namespace =
        get((connection) -> store.getById(namespaceId, connection), format, namespaceId);
    if (namespace == null) {
      throw notFoundException("Namespace not found with id: " + namespaceId);
    }
    return namespace;
  }

  @Override
  protected void doCreate(Namespace namespace, Connection connection)
      throws PathException, SQLException, IOException {
    // validate whether namespace name or alias name already exist
    validateNameOrAliasDuplication(null, namespace, connection);

    store.create(namespace, connection);

    // add human creator as a member to namespace, if managed natively
    if (!Utils.isAthensManaged(namespace)) {
      memberService.addNamespaceMemberWithoutCommit(
          namespace.getId(), Arrays.asList(namespace.getCreatedBy()), connection);
    }

    // create home folder for namespace
    List<Folder> folders = new ArrayList<>();
    Path path = Path.getPathByNamespace(namespace.getAlias());
    createHomeFolder(path, namespace.getCreatedBy(), namespace.getCreatedTime(), folders);
    folderStore.createFolder(folders, connection);
  }

  @Override
  protected void doCreates(
      final List<Namespace> namespaces, final Connection connection, final String principal)
      throws PathException, IOException, SQLException {

    // TODO validateNameOrAliasDuplication
    store.create(namespaces, connection);

    List<Integer> nativeNamespaceIds =
        namespaces.stream()
            .filter(namespace -> !Utils.isAthensManaged(namespace))
            .map(Namespace::getId)
            .collect(Collectors.toList());
    memberService.addNamespaceMember(nativeNamespaceIds, Arrays.asList(principal), connection);

    List<Folder> folders = new ArrayList<>();
    for (Namespace namespace : namespaces) {
      Path path = Path.getPathByNamespace(namespace.getAlias());
      createHomeFolder(path, namespace.getCreatedBy(), namespace.getCreatedTime(), folders);
    }
    folderStore.createFolder(folders, connection);
  }

  @Override
  protected void doUpdates(
      List<Namespace> newNamespaces, Connection connection)
      throws IOException, SQLException {
    Map<Integer, Namespace> newNamespaceMap =
        newNamespaces.stream().collect(Collectors.toMap(Namespace::getId, Function.identity()));
    Set<Integer> ids = newNamespaceMap.keySet();
    List<Namespace> oldNamespaces = store.getByIds(new ArrayList<>(ids), connection);

    String principal = newNamespaces.get(0).getUpdatedBy();

    for (Namespace oldNamespace : oldNamespaces) {

      // check user or service authorized to update
      if (!authService.authorize(oldNamespace, principal)) {
        throw forbiddenException("Namespace admin only allowed to update namespace.");
      }
      // the list of newNamespaces and oldNamespaces could be out of order. So, lookup the
      // corresponding newNamespace from the map.
      updateFields(oldNamespace, newNamespaceMap.get(oldNamespace.getId()));
    }
    store.update(oldNamespaces, connection);
  }

  @Override
  protected void setCreatorIdAndTime(Namespace namespace, String principal, Timestamp timestamp) {
    namespace.setCreatedBy(principal);
    namespace.setCreatedTime(timestamp);
  }

  @Override
  protected void setUpdaterIdAndTime(Namespace namespace, String principal, Timestamp timestamp) {
    namespace.setUpdatedBy(principal);
    namespace.setUpdatedTime(timestamp);
  }

  @Override
  protected void preCreate(Namespace namespace) {
    autofillAliasIfNotPresent(namespace);
  }

  public Namespace update(int namespaceId, Namespace namespace, HttpServletRequest request) {
    Namespace updated;
    try (final Connection con = store.getReadWriteConnection()) {
      try {
        Namespace savedNamespace = store.getById(namespaceId, con);
        updated = doUpdate(savedNamespace, namespace, request, con);
        store.commit(con);
      } catch (Exception e) {
        store.rollback(con);
        throw e;
      }
    } catch (SQLException | IOException exception) {
      String message = "Error updating Namespace with id: " + namespaceId;
      LOGGER.error(message, exception);
      throw internalServerError(message);
    }
    return updated;
  }

  public Namespace update(
      String name, String alias, Namespace namespace, HttpServletRequest request) {
    if (Strings.isNullOrEmpty(name) && Strings.isNullOrEmpty(alias)) {
      throw badRequestException("Namespace name/alias name should be provided");
    }

    Namespace updated;
    try (final Connection connection = store.getReadWriteConnection()) {
      try {
        Namespace savedNamespace =
            name != null
                ? store.getNamespaceByName(name, connection)
                : store.getNamespaceByAlias(alias, connection);
        updated = doUpdate(savedNamespace, namespace, request, connection);
        store.commit(connection);
      } catch (Exception e) {
        store.rollback(connection);
        throw e;
      }
    } catch (SQLException | IOException exception) {
      String message = "Error updating namespace: " + name;
      LOGGER.error(message, exception);
      throw internalServerError(message);
    }
    return updated;
  }

  public Namespace doUpdate(
      Namespace original, Namespace modified, HttpServletRequest request, Connection connection)
      throws SQLException, IOException {
    if (original == null) {
      throw notFoundException("Namespace not found");
    }

//    Principal principal = AuthUtil.extractYamasPrincipal(request).getPrincipal();
    String principal = request.getUserPrincipal().getName();

    // check user or service authorized to make changes
    if (!authService.authorize(original, principal)) {
      throw forbiddenException("Namespace admin only allowed to modify namespace.");
    }

    // override fields
    updateFields(original, modified);

    original.setUpdatedBy(principal);
    original.setUpdatedTime(new Timestamp(System.currentTimeMillis()));

    // check namespace or alias already exist excluding current namespace
    validateNameOrAliasDuplication(original.getId(), original, connection);

    return store.update(original, connection);
  }

  private void updateFields(Namespace original, Namespace modified) {
    if (!isNullOrEmpty(modified.getAlias())) {
      original.setAlias(modified.getAlias());
    }
    if (modified.getMeta() != null) {
      original.setMeta(modified.getMeta());
    }
    if (modified.getEnabled() != null) {
      original.setEnabled(modified.getEnabled());
    }
    if (!isNullOrEmpty(modified.getUpdatedBy())) {
      original.setUpdatedBy(modified.getUpdatedBy());
    }
    if (modified.getUpdatedTime() != null) {
      original.setUpdatedTime(modified.getUpdatedTime());
    }
  }

  private void validateNameOrAliasDuplication(
      Integer namespaceIdToExclude, Namespace namespace, Connection connection)
      throws IOException, SQLException {
    final String name = namespace.getName(), alias = namespace.getAlias();

    final List<Namespace> existingNamespace =
        getMatchingNamespaceOrAlias(namespaceIdToExclude, name, alias, connection);

    if (!existingNamespace.isEmpty()) {
      final Set<String> existingNameSet =
          existingNamespace.stream().map(Namespace::getName).collect(Collectors.toSet());
      final Set<String> existingAliasSet =
          existingNamespace.stream().map(Namespace::getAlias).collect(Collectors.toSet());

      StringBuilder builder = new StringBuilder();
      if (existingAliasSet.contains(name) || existingNameSet.contains(name)) {
        builder.append("Namespace name");
      }

      if (existingAliasSet.contains(alias) || existingNameSet.contains(alias)) {
        builder.append((builder.length() > 0) ? " and alias name" : "Namespace alias name");
      }

      builder.append(" is already taken !");
      throw badRequestException(builder.toString());
    }
  }

  private List<Namespace> getMatchingNamespaceOrAlias(
      Integer namespaceIdToExclude, String name, String alias, Connection connection)
      throws IOException, SQLException {
    List<Namespace> existingNamespace;
    existingNamespace =
        namespaceIdToExclude != null
            ? store.getMatchedNamespace(namespaceIdToExclude, name, alias, connection)
            : store.getMatchedNamespace(name, alias, connection);
    return existingNamespace;
  }

  private void autofillAliasIfNotPresent(Namespace namespace) {
    if (isNullOrEmpty(namespace.getAlias())) { // auto fill alias name
      String slug = slugify(namespace.getName());
      namespace.setAlias(slug);
    } else {
      namespace.setAlias(slugify(namespace.getAlias()));
    }
  }
}
