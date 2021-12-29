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
import net.opentsdb.horizon.fs.Path;
import net.opentsdb.horizon.fs.Path.PathException;
import net.opentsdb.horizon.fs.Path.RootType;
import net.opentsdb.horizon.fs.model.Content;
import net.opentsdb.horizon.fs.model.File;
import net.opentsdb.horizon.fs.model.FileHistory;
import net.opentsdb.horizon.fs.model.Folder;
import net.opentsdb.horizon.fs.store.FolderStore;
import net.opentsdb.horizon.fs.view.FileDto;
import net.opentsdb.horizon.fs.view.FolderDto;
import net.opentsdb.horizon.fs.view.FolderType;
import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.model.User;
import net.opentsdb.horizon.store.NamespaceFollowerStore;
import net.opentsdb.horizon.store.UserStore;
import net.opentsdb.horizon.view.MoveRequest;
import net.opentsdb.horizon.view.NamespaceFolderDto;
import net.opentsdb.horizon.view.UserFolderDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static net.opentsdb.horizon.service.BaseService.badRequestException;
import static net.opentsdb.horizon.service.BaseService.forbiddenException;
import static net.opentsdb.horizon.service.BaseService.internalServerError;
import static net.opentsdb.horizon.service.BaseService.notFoundException;
import static net.opentsdb.horizon.util.Utils.compress;
import static net.opentsdb.horizon.util.Utils.deSerialize;
import static net.opentsdb.horizon.util.Utils.decompress;
import static net.opentsdb.horizon.util.Utils.isNullOrEmpty;
import static net.opentsdb.horizon.util.Utils.serialize;
import static net.opentsdb.horizon.util.Utils.slugify;

public class DashboardService {

  private static final Logger LOGGER = LoggerFactory.getLogger(DashboardService.class);

  public static String SO_SERVICE = "HZ_DASHBOARD_SERVICE";

  private final FolderStore folderStore;
  private final NamespaceMemberService namespaceMemberService;
  private final NamespaceFollowerStore namespaceFollowerStore;
  private final NamespaceCache namespaceCache;
  private final AuthService authService;
  private UserStore userStore;
  private final MessageDigest digest;
  private DashboardActivityJobScheduler activityJobScheduler;

  public DashboardService(
      final FolderStore folderStore,
      final NamespaceMemberService namespaceMemberService,
      final NamespaceFollowerStore namespaceFollowerStore,
      NamespaceCache namespaceCache,
      final AuthService authService,
      final UserStore userStore,
      final MessageDigest digest,
      final DashboardActivityJobScheduler activityJobScheduler) {

    this.folderStore = folderStore;
    this.namespaceMemberService = namespaceMemberService;
    this.namespaceFollowerStore = namespaceFollowerStore;
    this.namespaceCache = namespaceCache;
    this.authService = authService;
    this.userStore = userStore;
    this.digest = digest;
    this.activityJobScheduler = activityJobScheduler;
  }

  public FolderDto createFolder(FolderDto view, HttpServletRequest request) {

    Folder model = viewToModel(view);

    String errorMessage = "Error creating dashboard folder";
    try (Connection connection = folderStore.getReadWriteConnection()) {

      try {
        prepareFolder(view, request, model, connection);

        long id = folderStore.createFolder(model, connection);
        model.setId(id);

        folderStore.commit(connection);
        modelToView(model, view);
        return view;
      } catch (Exception e) {
        folderStore.rollback(connection);
        throw e;
      }
    } catch (SQLException e) {
      LOGGER.error(errorMessage, e);
      throw internalServerError(errorMessage);
    } catch (PathException e) {
      LOGGER.error(errorMessage, e);
      throw badRequestException(e.getMessage());
    }
  }

  public FolderDto createFile(FileDto view, HttpServletRequest request) {
    File model = viewToModel(view);

    String errorMessage = "Error creating dashboard";
    try (Connection connection = folderStore.getReadWriteConnection()) {

      try {
        prepareFolder(view, request, model, connection);
        Content content = createContent(view.getContent());
        content.setCreatedby(model.getCreatedBy());
        content.setCreatedtime(model.getCreatedTime());
        model.setContentid(content.getSha2());

        folderStore.createContent(content, connection);
        long id = folderStore.createFile(model, connection);
        model.setId(id);

        FileHistory fileHistory = createFileHistory(model);
        fileHistory.setCreatedtime(model.getCreatedTime());
        folderStore.createFileHistory(fileHistory, connection);

        folderStore.commit(connection);
      } catch (Exception e) {
        folderStore.rollback(connection);
        throw e;
      }

      modelToView(model, view);
      return view;
    } catch (SQLException | IOException e) {
      LOGGER.error(errorMessage, e);
      throw internalServerError(errorMessage);
    } catch (PathException e) {
      LOGGER.error(errorMessage, e);
      throw badRequestException(e.getMessage());
    }
  }

  public Response createOrUpdateFolder(FolderDto folder, HttpServletRequest request) {
    Long id = folder.getId();
    if (id == null) {
      return Response.status(Status.CREATED).entity(createFolder(folder, request)).build();
    } else {
      return Response.status(Status.OK).entity(updateFolder(folder, request)).build();
    }
  }

  public Response createOrUpdateFile(FileDto file, HttpServletRequest request) {
    Long id = file.getId();
    if (id == null) {
      return Response.status(Status.CREATED).entity(createFile(file, request)).build();
    } else {
      return Response.status(Status.OK).entity(updateFile(file, request)).build();
    }
  }

  public FolderDto moveFolder(MoveRequest moveRequest, HttpServletRequest request) {
    String userId = request.getUserPrincipal().getName();

    String errorMessage = "Error moving dashboard folder";
    try (Connection con = folderStore.getReadWriteConnection()) {

      Folder sourceFolder;
      try {
        sourceFolder =
            folderStore.getFileOrFolderById(FolderType.DASHBOARD, moveRequest.getSourceId(), con);
        if (sourceFolder == null) {
          String message = "Source id not found: " + moveRequest.getSourceId();
          throw notFoundException(message);
        }
        Path sourcePath = Path.get(sourceFolder.getPath());

        checkAccess(sourcePath, userId);

        Folder destinationFolder =
            folderStore.getFileOrFolderById(
                FolderType.DASHBOARD, moveRequest.getDestinationId(), con);
        if (destinationFolder == null) {
          String message = "Destination id not found: " + moveRequest.getDestinationId();
          throw notFoundException(message);
        }
        if (destinationFolder.isFile()) {
          String message = "Destination is not a folder";
          throw badRequestException(message);
        }
        Path destinationPath = Path.get(destinationFolder.getPath());

        checkAccess(destinationPath, userId);

        if (sourcePath.equals(destinationPath)) {
          return modelToView(sourceFolder);
        }

        if (sourcePath.isAncestor(destinationPath)) {
          String message = "Can't move ancestor folder to descendant";
          throw badRequestException(message);
        }

        byte[] oldSourceFolderPathHash = sourceFolder.getPathHash();
        sourceFolder.setPath(Path.getChildPath(destinationPath.getPath(), sourcePath.getLeaf()));

        List<Folder> existingSubFolders =
            folderStore.listByParentPathHash(
                FolderType.DASHBOARD, destinationFolder.getPathHash(), con);
        if (existingSubFolders.contains(sourceFolder)) {
          String newName = "Copy of " + sourceFolder.getName();
          sourceFolder.setName(newName);
        }
        String newPath =
            Path.getChildPath(destinationPath.getPath(), slugify(sourceFolder.getName()));
        sourceFolder.setPath(newPath);
        sourceFolder.setPathHash(Path.hash(newPath));
        sourceFolder.setParentPathHash(destinationFolder.getPathHash());
        sourceFolder.setUpdatedBy(userId);
        sourceFolder.setUpdatedTime(new Timestamp(System.currentTimeMillis()));

        updatePathRecursively(sourceFolder, oldSourceFolderPathHash, con);

        folderStore.updateFolder(sourceFolder, con);
        folderStore.commit(con);
      } catch (Exception e) {
        folderStore.rollback(con);
        throw e;
      }

      return modelToView(sourceFolder);

    } catch (PathException e) {
      LOGGER.error(errorMessage, e);
      throw badRequestException(e.getMessage());
    } catch (SQLException e) {
      LOGGER.error(errorMessage, e);
      throw internalServerError(errorMessage);
    }
  }

  private FolderDto updateFolder(FolderDto folder, HttpServletRequest request) {

    String userId = request.getUserPrincipal().getName();

    String errorMessage = "Error updating dashboard file";
    try (Connection con = folderStore.getReadWriteConnection()) {
      Long id = folder.getId();
      Folder oldFolder;
      try {
        oldFolder = folderStore.getFolderById(FolderType.DASHBOARD, id, userId, con);
        if (oldFolder == null) {
          String message = "Folder not found with id: " + id;
          throw notFoundException(message);
        }

        String oldPath = oldFolder.getPath();
        Path path = Path.get(oldPath);
        checkAccess(path, userId);

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        String newName = folder.getName();
        if (!isNullOrEmpty(newName) && !newName.equals(oldFolder.getName())) {
          byte[] oldPathHash = oldFolder.getPathHash();
          oldFolder.setName(newName);
          path.stLeaf(slugify(newName));
          oldFolder.setPath(path.getPath());
          oldFolder.setPathHash(path.hash());
          oldFolder.setUpdatedBy(userId);
          oldFolder.setUpdatedTime(timestamp);
          updatePathRecursively(oldFolder, oldPathHash, con);
          folderStore.updateFolder(oldFolder, con);
        }

        folderStore.commit(con);
      } catch (Exception e) {
        folderStore.rollback(con);
        throw e;
      }
      return modelToView(oldFolder);

    } catch (SQLException e) {
      LOGGER.error(errorMessage, e);
      throw internalServerError(errorMessage);
    } catch (PathException e) {
      LOGGER.error(errorMessage, e);
      throw badRequestException(e.getMessage());
    }
  }

  private void updatePathRecursively(
      Folder parentFolder, byte[] oldParentPathHash, Connection connection) throws SQLException {

    List<Folder> subFolders =
        folderStore.listByParentPathHash(FolderType.DASHBOARD, oldParentPathHash, connection);
    for (Folder subFolder : subFolders) {
      byte[] oldPathHash = subFolder.getPathHash();
      String childSlug = slugify(subFolder.getName());
      String newChildPath = Path.getChildPath(parentFolder.getPath(), childSlug);
      subFolder.setPath(newChildPath);
      subFolder.setPathHash(Path.hash(newChildPath));
      subFolder.setParentPathHash(parentFolder.getPathHash());
      subFolder.setUpdatedBy(parentFolder.getUpdatedBy());
      subFolder.setUpdatedTime(parentFolder.getUpdatedTime());

      if (!subFolder.isFile()) {
        updatePathRecursively(subFolder, oldPathHash, connection);
      }
      folderStore.updateFolder(subFolder, connection);
    }
  }

  private FileDto updateFile(FileDto file, HttpServletRequest request) {

    String userId = request.getUserPrincipal().getName();

    String errorMessage = "Error updating dashboard file";
    try (Connection con = folderStore.getReadWriteConnection()) {
      try {
        Long id = file.getId();
        File oldFile = folderStore.getFileById(FolderType.DASHBOARD, id, con);
        if (oldFile == null) {
          String message = "File not found with id: " + id;
          throw notFoundException(message);
        }

        Path path = Path.get(oldFile.getPath());
        checkAccess(path, userId);

        String newName = file.getName();
        boolean updated = false;

        if (!isNullOrEmpty(newName) && !newName.equals(oldFile.getName())) {
          oldFile.setName(newName);
          String slug = slugify(newName);
          path.stLeaf(slug);
          oldFile.setPath(path.getPath());
          oldFile.setPathHash(path.hash());

          updated = true;
        }

        Content newContent = createContent(file.getContent());
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        if (!Arrays.equals(oldFile.getContentid(), newContent.getSha2())) {
          newContent.setCreatedby(userId);
          newContent.setCreatedtime(timestamp);
          folderStore.createContent(newContent, con);

          oldFile.setContent(newContent.getData());
          oldFile.setContentid(newContent.getSha2());

          FileHistory fileHistory = createFileHistory(oldFile);
          fileHistory.setCreatedtime(timestamp);
          folderStore.createFileHistory(fileHistory, con);

          updated = true;
        }

        if (updated) {
          oldFile.setUpdatedBy(userId);
          oldFile.setUpdatedTime(timestamp);
          folderStore.updateFile(oldFile, con);
        }

        folderStore.commit(con);
        return modelToView(oldFile);
      } catch (Exception e) {
        folderStore.rollback(con);
        throw e;
      }

    } catch (PathException e) {
      LOGGER.error(errorMessage, e);
      throw badRequestException(e.getMessage());
    } catch (SQLException | IOException e) {
      LOGGER.error(errorMessage, e);
      throw internalServerError(errorMessage);
    }
  }

  public FolderDto getFolderById(final long id, final String userId) {
    try (Connection connection = folderStore.getReadOnlyConnection()) {
      Folder model = folderStore.getFolderById(FolderType.DASHBOARD, id, userId, connection);
      if (null == model) {
        throw notFoundException("Folder not found with id: " + id);
      }
      FolderDto folderDto = modelToView(model);
      folderDto.setFavorite(model.getFavoritedTime() != null);
      return folderDto;
    } catch (SQLException sqlException) {
      String message = "Error reading folder id: " + id;
      LOGGER.error(message, sqlException);
      throw internalServerError(message);
    }
  }

  public FileDto getFileById(final long id, final String userId) {
    try (Connection connection = folderStore.getReadOnlyConnection()) {
      File model = folderStore.getFileAndContentById(FolderType.DASHBOARD, id, connection);
      if (null == model) {
        throw notFoundException("Dashboard not found with id: " + id);
      }
      boolean favorite = folderStore.isFavorite(userId, id, connection);
      activityJobScheduler.addActivity(id, userId);
      FileDto view = modelToView(model);
      view.setFavorite(favorite);
      return view;
    } catch (SQLException | IOException e) {
      String message = "Error reading dashboard with id: " + id;
      LOGGER.error(message, e);
      throw internalServerError(message);
    }
  }

  public FolderDto getByPath(final String pathString, final String userId) {

    String idString = pathString.split("/")[pathString.startsWith("/") ? 1 : 0];
    if (isNullOrEmpty(idString)) {
      throw badRequestException("Invalid path: " + pathString);
    }

    long id;
    try {
      id = Long.parseLong(idString);
    } catch (NumberFormatException e) {
      throw badRequestException("Invalid path: " + pathString);
    }

    try (Connection connection = folderStore.getReadOnlyConnection()) {
      Folder rootFolder = folderStore.getFileOrFolderById(FolderType.DASHBOARD, id, connection);
      if (rootFolder == null) {
        throw badRequestException("Path not found " + pathString);
      }

      boolean favorite = folderStore.isFavorite(userId, id, connection);
      if (rootFolder.isFile()) {
        activityJobScheduler.addActivity(id, userId);
        FolderDto view = modelToView(rootFolder);
        view.setFavorite(favorite);
        return view;
      } else {
        List<Folder> subFolders =
            folderStore.listByParentPathHash(
                FolderType.DASHBOARD, rootFolder.getPathHash(), connection);
        FolderDto rootFolderDto = modelToView(rootFolder);
        rootFolderDto.setFavorite(favorite);

        List<FolderDto> subFolderDTOs = new ArrayList<>();
        List<FolderDto> fileDTOs = new ArrayList<>();
        for (Folder subFolder : subFolders) {
          FolderDto subFolderDTO = modelToView(subFolder);
          if (subFolder.isFile()) {
            fileDTOs.add(subFolderDTO);
          } else {
            subFolderDTOs.add(subFolderDTO);
          }
        }
        rootFolderDto.setSubfolders(subFolderDTOs);
        rootFolderDto.setFiles(fileDTOs);
        return rootFolderDto;
      }
    } catch (SQLException e) {
      String message = "Error listing for path: " + pathString;
      LOGGER.error(message, e);
      throw internalServerError(message);
    }
  }

  private FolderDto getByPath(Path path, Connection connection) throws SQLException, IOException {

    byte[] pathHash = path.hash();
    File file = folderStore.getFileAndContentByPathHash(FolderType.DASHBOARD, pathHash, connection);
    if (file == null) {
      return null; // path not found
    }

    if (file.isFile()) {
      return modelToView(file);
    }

    Folder rootFolder = file; // it's a folder
    List<Folder> subFolders =
        folderStore.listByParentPathHash(FolderType.DASHBOARD, pathHash, connection);
    FolderDto rootFolderDto = modelToView(rootFolder);

    List<FolderDto> subFolderDtos = new ArrayList<>();
    List<FolderDto> fileDtos = new ArrayList<>();
    for (Folder subFolder : subFolders) {
      FolderDto folderDto = modelToView(subFolder);
      if (subFolder.isFile()) {
        fileDtos.add(folderDto);
      } else {
        subFolderDtos.add(folderDto);
      }
    }
    rootFolderDto.setSubfolders(subFolderDtos);
    rootFolderDto.setFiles(fileDtos);
    return rootFolderDto;
  }

  public FolderDto getNamespaceFolder(String namespace) {
    try (Connection connection = folderStore.getReadOnlyConnection()) {
      Path path = Path.getPathByNamespace(namespace);
      return getByPath(path, connection);

    } catch (PathException e) {
      throw badRequestException(e.getMessage());
    } catch (SQLException | IOException e) {
      String message = "Error reading namespace folder";
      LOGGER.error(message, e);
      throw internalServerError(message);
    }
  }

  public UserFolderDto getUserFolder(String userId) {
    try (Connection connection = folderStore.getReadOnlyConnection()) {
      final UserFolderDto userFolder = new UserFolderDto();

      User user = userStore.getNameById(userId, connection);
      if (user == null) {
        throw notFoundException("User not found with id: " + userId);
      }

      userFolder.setUser(user);

      // obtain user level folder
      Path path = Path.getByUserId(userId);
      userFolder.setPersonalFolder(getByPath(path, connection));

      // obtain user's namespace membership folder
      final List<Namespace> memberNamespaces =
          namespaceMemberService.getNamespaces(userId, connection);
      for (Namespace ns : memberNamespaces) {
        final Path namespacePath = Path.getPathByNamespace(ns.getAlias());
        final FolderDto folderDto = getByPath(namespacePath, connection);
        NamespaceFolderDto namespaceFolderDto = new NamespaceFolderDto();
        namespaceFolderDto.setNamespace(ns);
        namespaceFolderDto.setFolder(folderDto);
        userFolder.addToMemberNamespaces(namespaceFolderDto);
      }

      // obtain user's namespace followership folder
      final List<Namespace> followingNamespaces =
          namespaceFollowerStore.getFollowingNamespaces(userId, connection);
      for (Namespace ns : followingNamespaces) {
        final Path namespacePath = Path.getPathByNamespace(ns.getAlias());
        final FolderDto folderDto = getByPath(namespacePath, connection);
        NamespaceFolderDto namespaceFolderDto = new NamespaceFolderDto();
        namespaceFolderDto.setNamespace(ns);
        namespaceFolderDto.setFolder(folderDto);
        userFolder.addToFollowerNamespaces(namespaceFolderDto);
      }

      return userFolder;

    } catch (PathException e) {
      throw badRequestException(e.getMessage());
    } catch (SQLException | IOException e) {
      String message = "Error reading folders for user: " + userId;
      LOGGER.error(message, e);
      throw internalServerError(message);
    }
  }

  public UserFolderDto getRecentlyVisited(final String userId, final int limit) {
    try (Connection connection = folderStore.getReadOnlyConnection()) {
      final UserFolderDto userFolder = new UserFolderDto();

      User user = userStore.getNameById(userId, connection);
      if (user == null) {
        throw notFoundException("User not found with id: " + userId);
      }
      userFolder.setUser(user);
      List<Folder> foldersAndFiles = folderStore.getRecentlyVisited(userId, limit, connection);
      List<FolderDto> folderDtos =
          foldersAndFiles.stream().map(model -> modelToView(model)).collect(Collectors.toList());
      userFolder.setRecent(folderDtos);
      return userFolder;
    } catch (SQLException e) {
      String message = "Error reading recently visited dashboards for user: " + userId;
      LOGGER.error(message, e);
      throw internalServerError(message);
    }
  }

  public UserFolderDto getFavorites(final String userId) {
    try (Connection connection = folderStore.getReadOnlyConnection()) {
      final UserFolderDto userFolder = new UserFolderDto();

      User user = userStore.getNameById(userId, connection);
      if (user == null) {
        throw notFoundException("User not found with id: " + userId);
      }
      userFolder.setUser(user);
      List<Folder> foldersAndFiles = folderStore.getFavorites(userId, connection);
      List<FolderDto> folderDtos =
          foldersAndFiles.stream().map(model -> modelToView(model)).collect(Collectors.toList());
      userFolder.setFavorites(folderDtos);
      return userFolder;
    } catch (SQLException e) {
      String message = "Error reading favorite dashboards and folders for user: " + userId;
      LOGGER.error(message, e);
      throw internalServerError(message);
    }
  }

  public void addToFavorites(final long id, final String userId) {
    try (Connection connection = folderStore.getReadWriteConnection()) {
      Folder folder = folderStore.getById(FolderType.DASHBOARD, id, connection);
      if (folder == null) {
        throw notFoundException("Folder not found with id: " + id);
      }
      User user = userStore.getNameById(userId, connection);
      if (user == null) {
        throw notFoundException("User not found with id: " + userId);
      }
      folderStore.addToFavorites(id, userId, connection);
      folderStore.commit(connection);
    } catch (SQLException e) {
      String message = "Error adding to favorites of " + userId + " folder id: " + id;
      LOGGER.error(message, e);
      throw internalServerError(message);
    }
  }

  public void deleteFromFavorites(final long id, final String userId) {
    try (Connection connection = folderStore.getReadWriteConnection()) {
      Folder folder = folderStore.getById(FolderType.DASHBOARD, id, connection);
      if (folder == null) {
        throw notFoundException("Folder not found with id: " + id);
      }
      User user = userStore.getNameById(userId, connection);
      if (user == null) {
        throw notFoundException("User not found with id: " + userId);
      }
      folderStore.deleteFromFavorites(userId, id, connection);
      folderStore.commit(connection);
    } catch (SQLException e) {
      String message = "Error deleting from favorites of " + userId + " folder id: " + id;
      LOGGER.error(message, e);
      throw internalServerError(message);
    }
  }

  private FileHistory createFileHistory(File file) {
    FileHistory fileHistory = new FileHistory();
    fileHistory.setFileid(file.getId());
    fileHistory.setContentid(file.getContentid());
    return fileHistory;
  }

  private void prepareFolder(
      FolderDto view, HttpServletRequest request, Folder model, Connection connection)
      throws PathException, SQLException {
    String userId = request.getUserPrincipal().getName();
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    model.setCreatedBy(userId);
    model.setUpdatedBy(userId);
    model.setCreatedTime(timestamp);
    model.setUpdatedTime(timestamp);

    Long parentId = view.getParentId();
    Path parentPath;

    if (parentId == null) {
      parentPath = Path.getByUserId(userId);
    } else {
      Folder parent = folderStore.getFolderById(FolderType.DASHBOARD, parentId, userId, connection);
      if (null == parent || parent.isFile()) {
        throw badRequestException("Invalid parent id : " + parentId);
      }

      parentPath = Path.get(parent.getPath());
      checkAccess(parentPath, userId);
    }

    String slug = slugify(model.getName());
    String pathString = parentPath.getChildPath(slug);

    model.setParentPathHash(parentPath.hash());
    model.setSlug(slug);
    model.setPath(pathString);
    model.setPathHash(parentPath.hash(pathString));
  }

  private void checkAccess(Path path, String principal) throws SQLException {

    RootType rootType = path.getRootType();
    String rootName = path.getRootName();
    boolean hasAccess;
    if (rootType == RootType.user) {
      String userId = Path.getUserId(principal);
      hasAccess = rootName.equalsIgnoreCase(userId);
    } else if (rootType == RootType.namespace) {
      Namespace namespace;
      try {
        namespace = namespaceCache.getByName(rootName);
      } catch (Exception e) {
        String message = "Error reading namespace with name: " + rootName;
        LOGGER.error(message, e);
        throw internalServerError(message);
      }
      if (namespace == null) {
        throw notFoundException("Namespace not found with name: " + rootName);
      } else {
        hasAccess = authService.authorize(namespace, principal);
      }
    } else {
      throw new IllegalArgumentException("Invalid root type: " + rootType);
    }

    if (!hasAccess) {
      throw forbiddenException("Access denied to path: " + path.getPath());
    }
  }

  private FolderDto modelToView(Folder model) {
    FolderDto dto = new FolderDto();
    modelToView(model, dto);
    return dto;
  }

  private FileDto modelToView(File model) throws IOException {
    FileDto dto = new FileDto();
    modelToView(model, dto);
    byte[] compressed = model.getContent();
    if (null != compressed) {
      byte[] decompressed = decompress(compressed);
      Object deSerialized = deSerialize(decompressed, Object.class);
      dto.setContent(deSerialized);
    }
    return dto;
  }

  private void modelToView(Folder model, FolderDto view) {
    if (model.getId() > 0) {
      view.setId(model.getId());

      String slug = model.getSlug();
      if (slug == null) {
        slug = Path.getLeaf(model.getPath());
      }

      view.setPath("/" + model.getId() + "/" + slug);
    }
    view.setName(model.getName());
    view.setType(model.getType());
    view.setFullPath(model.getPath());
    view.setCreatedBy(model.getCreatedBy());
    view.setCreatedTime(model.getCreatedTime());
    view.setUpdatedBy(model.getUpdatedBy());
    view.setUpdatedTime(model.getUpdatedTime());
    view.setFavoritedTime(model.getFavoritedTime());
    view.setLastVisitedTime(model.getLastVisitedTime());
  }

  private Folder viewToModel(FolderDto view) {
    Folder model = new Folder();
    viewToModel(view, model);
    return model;
  }

  private File viewToModel(FileDto view) {
    File model = new File();
    viewToModel(view, model);
    return model;
  }

  private void viewToModel(FolderDto view, Folder model) {
    model.setId(view.getId() == null ? -1 : view.getId());
    model.setName(view.getName());
    model.setType(FolderType.DASHBOARD);
    model.setPath(view.getFullPath());
  }

  private Content createContent(Object content) throws IOException {
    byte[] serialized = serialize(content).getBytes();
    byte[] sha2 = digest.digest(serialized);
    return new Content(sha2, compress(serialized));
  }
}
