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

package net.opentsdb.horizon.fs.store;

import net.opentsdb.horizon.fs.model.Content;
import net.opentsdb.horizon.fs.model.File;
import net.opentsdb.horizon.fs.model.FileHistory;
import net.opentsdb.horizon.fs.model.Folder;
import net.opentsdb.horizon.fs.view.FolderType;
import net.opentsdb.horizon.service.BaseService;
import net.opentsdb.horizon.store.BaseStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static net.opentsdb.horizon.fs.store.ResultSetMapper.resultSetToFolderMapper;

public class FolderStore extends BaseStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(FolderStore.class);

  public FolderStore(final DataSource rwSrc, final DataSource roSrc) {
    super(rwSrc, roSrc);
  }

  String SQL_INSERT_FOLDER =
      "INSERT INTO folder (name, type, path, pathhash, parentpathhash, createdtime, createdby, updatedtime, updatedby ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

  public long createFolder(Folder folder, Connection connection) throws SQLException {
    long folderId = -1;
    try (PreparedStatement statement =
        connection.prepareStatement(SQL_INSERT_FOLDER, Statement.RETURN_GENERATED_KEYS)) {
      statement.setString(1, folder.getName());
      statement.setByte(2, folder.getType().value);
      statement.setString(3, folder.getPath());
      statement.setBytes(4, folder.getPathHash());
      statement.setBytes(5, folder.getParentPathHash());
      statement.setTimestamp(6, folder.getCreatedTime());
      statement.setString(7, folder.getCreatedBy());
      statement.setTimestamp(8, folder.getUpdatedTime());
      statement.setString(9, folder.getUpdatedBy());
      statement.executeUpdate();
      final ResultSet generatedKeys = statement.getGeneratedKeys();
      while (generatedKeys.next()) {
        folderId = generatedKeys.getLong(1);
      }
    }
    return folderId;
  }

  public int[] createFolder(List<Folder> folders, Connection connection) throws SQLException {
    int[] counts;
    try (PreparedStatement statement =
        connection.prepareStatement(SQL_INSERT_FOLDER, Statement.RETURN_GENERATED_KEYS)) {
      for (Folder folder : folders) {
        statement.setString(1, folder.getName());
        statement.setByte(2, folder.getType().value);
        statement.setString(3, folder.getPath());
        statement.setBytes(4, folder.getPathHash());
        statement.setBytes(5, folder.getParentPathHash());
        statement.setTimestamp(6, folder.getCreatedTime());
        statement.setString(7, folder.getCreatedBy());
        statement.setTimestamp(8, folder.getUpdatedTime());
        statement.setString(9, folder.getUpdatedBy());

        statement.addBatch();
      }
      counts = statement.executeBatch();
      if (!folders.isEmpty()) {
        final ResultSet generatedKeys = statement.getGeneratedKeys();
        int i = 0;
        while (generatedKeys.next()) {
          final int id = generatedKeys.getInt(1);
          folders.get(i++).setId(id);
        }
      }
    }
    return counts;
  }

  public long createFile(File file, Connection connection) throws SQLException {
    String sql =
        "INSERT INTO folder (name, type, path, pathhash, parentpathhash, contentid, createdtime, createdby, updatedtime, updatedby ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    long folderId = -1;
    try (PreparedStatement statement =
        connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
      statement.setString(1, file.getName());
      statement.setByte(2, file.getType().value);
      statement.setString(3, file.getPath());
      statement.setBytes(4, file.getPathHash());
      statement.setBytes(5, file.getParentPathHash());
      statement.setBytes(6, file.getContentid());
      statement.setTimestamp(7, file.getCreatedTime());
      statement.setString(8, file.getCreatedBy());
      statement.setTimestamp(9, file.getUpdatedTime());
      statement.setString(10, file.getUpdatedBy());
      statement.executeUpdate();
      final ResultSet generatedKeys = statement.getGeneratedKeys();
      while (generatedKeys.next()) {
        folderId = generatedKeys.getLong(1);
      }
    }
    return folderId;
  }

  public void updateFolder(Folder folder, Connection connection) throws SQLException {
    String sql =
        "UPDATE folder set name = ?, path = ?, pathhash = ?, parentpathhash = ?, updatedtime = ?, updatedby = ? where id = ?";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, folder.getName());
      statement.setString(2, folder.getPath());
      statement.setBytes(3, folder.getPathHash());
      statement.setBytes(4, folder.getParentPathHash());
      statement.setTimestamp(5, folder.getUpdatedTime());
      statement.setString(6, folder.getUpdatedBy());
      statement.setLong(7, folder.getId());
      statement.executeUpdate();
    }
  }

  public void updateFile(File file, Connection connection) throws SQLException {
    String sql =
        "UPDATE folder set name = ?, path = ?, pathhash = ?, parentpathhash = ?, contentid = ?, updatedtime = ?, updatedby = ? where id = ?";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, file.getName());
      statement.setString(2, file.getPath());
      statement.setBytes(3, file.getPathHash());
      statement.setBytes(4, file.getParentPathHash());
      statement.setBytes(5, file.getContentid());
      statement.setTimestamp(6, file.getUpdatedTime());
      statement.setString(7, file.getUpdatedBy());
      statement.setLong(8, file.getId());
      statement.executeUpdate();
    }
  }

  public Content createContent(Content content, Connection connection) throws SQLException {
    String sql =
        "INSERT INTO content (sha2, data, createdtime, createdby) SELECT ?, ?, ?, ? FROM (SELECT 1) l LEFT JOIN content r ON r.sha2 = ? WHERE r.sha2 IS NULL";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setBytes(1, content.getSha2());
      statement.setBytes(2, content.getData());
      statement.setTimestamp(3, content.getCreatedtime());
      statement.setString(4, content.getCreatedby());
      statement.setBytes(5, content.getSha2());
      statement.executeUpdate();
    }
    return content;
  }

  public long createFileHistory(FileHistory fileHistory, Connection connection)
      throws SQLException {

    String sql =
        "INSERT INTO folder_history (folderid, contentid, createdtime) SELECT ?, ?, ? FROM (SELECT 1) l LEFT JOIN folder_history r ON r.folderid = ? AND r.contentid = ? AND r.createdtime = ? WHERE r.folderid IS NULL AND r.contentid IS NULL AND r.createdtime IS NULL";
    long id = -1;
    try (PreparedStatement statement =
        connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
      statement.setLong(1, fileHistory.getFileid());
      statement.setBytes(2, fileHistory.getContentid());
      statement.setTimestamp(3, fileHistory.getCreatedtime());
      statement.setLong(4, fileHistory.getFileid());
      statement.setBytes(5, fileHistory.getContentid());
      statement.setTimestamp(6, fileHistory.getCreatedtime());
      statement.executeUpdate();

      final ResultSet generatedKeys = statement.getGeneratedKeys();
      while (generatedKeys.next()) {
        id = generatedKeys.getLong(1);
      }
    }
    return id;
  }

  public Folder getById(FolderType type, long id, Connection connection) throws SQLException {
    final String sql = "SELECT * from folder WHERE type = ? AND id = ?";
    Folder folder = null;
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setByte(1, type.value);
      statement.setLong(2, id);
      try (final ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          folder = resultSetToFolderMapper(resultSet);
        }
      }
    }
    return folder;
  }

  private static final String GET_FOLDER_BY_ID =
      "SELECT f.id, f.name, f.type, f.path, f.pathhash, f.parentpathhash, f.contentid, f.createdtime, f.createdby, "
          + "f.updatedtime, f.updatedby, ff.createdtime as favoritedtime FROM folder f "
          + "LEFT OUTER JOIN favorite_folder ff ON f.id = ff.folderid AND ff.userid = ? "
          + "WHERE f.type = ? AND f.id = ? AND f.contentid IS NULL";

  public Folder getFolderById(
      FolderType folderType, long folderId, String userId, Connection connection)
      throws SQLException {
    Folder folder = null;
    try (PreparedStatement statement = connection.prepareStatement(GET_FOLDER_BY_ID)) {
      statement.setString(1, userId);
      statement.setByte(2, folderType.value);
      statement.setLong(3, folderId);
      try (final ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
          folder = resultSetToFolderMapper(rs);
          folder.setFavoritedTime(rs.getTimestamp("favoritedtime"));
        }
      }
    }
    return folder;
  }

  public Folder getFileOrFolderById(FolderType folderType, long id, Connection connection)
      throws SQLException {
    String sql =
        "SELECT f.id, f.name, f.type, f.path, f.pathhash, f.parentpathhash, f.contentid, f.createdtime, f.createdby, f.updatedtime, f.updatedby, c.data "
            + "FROM folder f LEFT OUTER JOIN content c ON f.contentid = c.sha2 "
            + "WHERE f.type = ? AND f.id = ?";
    return getFileAndContentById(folderType, id, connection, sql);
  }

  public Folder getFolderByPathHash(FolderType folderType, byte[] pathHash, Connection connection)
      throws SQLException {
    String sql = "SELECT * FROM folder WHERE type = ? AND pathhash = ?";
    Folder folder = null;
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setByte(1, folderType.value);
      statement.setBytes(2, pathHash);
      try (final ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
          folder = resultSetToFolderMapper(rs);
        }
      }
    }
    return folder;
  }

  public File getFileAndContentById(FolderType folderType, long id, Connection connection)
      throws SQLException {
    String sql =
        "SELECT f.id, f.name, f.type, f.path, f.pathhash, f.parentpathhash, f.contentid, f.createdtime, f.createdby, f.updatedtime, f.updatedby, c.data "
            + "FROM folder f INNER JOIN content c ON f.contentid = c.sha2 "
            + "WHERE f.type = ? AND f.id = ?";
    return getFileAndContentById(folderType, id, connection, sql);
  }

  private File getFileAndContentById(
      FolderType folderType, long id, Connection connection, String sql) throws SQLException {
    File file = null;
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setByte(1, folderType.value);
      statement.setLong(2, id);
      try (final ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
          file = new File();
          file.setId(rs.getLong("id"));
          file.setName(rs.getString("name"));
          file.setType(FolderType.values()[rs.getInt("type")]);
          file.setPath(rs.getString("path"));
          file.setPathHash(rs.getBytes("pathhash"));
          file.setParentPathHash(rs.getBytes("parentpathhash"));
          file.setContentid(rs.getBytes("contentid"));
          file.setCreatedTime(rs.getTimestamp("createdtime"));
          file.setCreatedBy(rs.getString("createdby"));
          file.setUpdatedTime(rs.getTimestamp("updatedtime"));
          file.setUpdatedBy(rs.getString("updatedby"));
          file.setContent(rs.getBytes("data"));
        }
      }
    }
    return file;
  }

  public File getFileById(FolderType folderType, long id, Connection connection)
      throws SQLException {
    String sql = "SELECT * FROM folder WHERE type = ? AND id = ? AND contentid IS NOT NULL";
    File file = null;
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setByte(1, folderType.value);
      statement.setLong(2, id);
      try (final ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
          file = new File();
          file.setId(rs.getLong("id"));
          file.setName(rs.getString("name"));
          file.setType(FolderType.values()[rs.getInt("type")]);
          file.setPath(rs.getString("path"));
          file.setPathHash(rs.getBytes("pathhash"));
          file.setParentPathHash(rs.getBytes("parentpathhash"));
          file.setContentid(rs.getBytes("contentid"));
          file.setCreatedTime(rs.getTimestamp("createdtime"));
          file.setCreatedBy(rs.getString("createdby"));
          file.setUpdatedTime(rs.getTimestamp("updatedtime"));
          file.setUpdatedBy(rs.getString("updatedby"));
        }
      }
    }
    return file;
  }

  public List<Folder> listByParentPathHash(
      FolderType folderType, byte[] parentPathHash, Connection connection) throws SQLException {
    String sql = "SELECT * FROM folder WHERE type = ? AND parentpathhash = ?";
    List<Folder> folders = new ArrayList();
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setByte(1, folderType.value);
      statement.setBytes(2, parentPathHash);
      try (final ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
          Folder folder = new Folder();
          folder.setId(rs.getLong("id"));
          folder.setName(rs.getString("name"));
          folder.setType(FolderType.values()[rs.getByte("type")]);
          folder.setPath(rs.getString("path"));
          folder.setPathHash(rs.getBytes("pathhash"));
          folder.setParentPathHash(rs.getBytes("parentpathhash"));
          folder.setContentid(rs.getBytes("contentid"));
          folder.setCreatedTime(rs.getTimestamp("createdtime"));
          folder.setCreatedBy(rs.getString("createdby"));
          folder.setUpdatedTime(rs.getTimestamp("updatedtime"));
          folder.setUpdatedBy(rs.getString("updatedby"));
          folders.add(folder);
        }
      }
    }
    return folders;
  }

  public File getFileAndContentByPathHash(
      FolderType folderType, byte[] pathHash, Connection connection) throws SQLException {
    String sql =
        "SELECT f.id, f.name, f.type, f.path, f.contentid, f.createdtime, f.createdby, f.updatedtime, f.updatedby, c.data "
            + "FROM folder f LEFT JOIN content c ON f.contentid = c.sha2 "
            + "WHERE f.type = ? AND f.pathhash = ?";
    File file = null;
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setByte(1, folderType.value);
      statement.setBytes(2, pathHash);
      try (final ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
          file = new File();
          file.setId(rs.getLong("id"));
          file.setName(rs.getString("name"));
          file.setType(FolderType.values()[rs.getInt("type")]);
          file.setPath(rs.getString("path"));
          file.setContentid(rs.getBytes("contentid"));
          file.setContent(rs.getBytes("data"));
          file.setCreatedTime(rs.getTimestamp("createdtime"));
          file.setCreatedBy(rs.getString("createdby"));
          file.setUpdatedTime(rs.getTimestamp("updatedtime"));
          file.setUpdatedBy(rs.getString("updatedby"));
        }
      }
    }
    return file;
  }

  public Content getContentById(byte[] sha2, Connection connection) throws SQLException {
    String sql = "SELECT data FROM content WHERE sha2 = ?";
    Content content = null;
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setBytes(1, sha2);
      try (final ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
          byte[] data = rs.getBytes("data");
          content = new Content();
          content.setData(data);
          content.setSha2(sha2);
        }
      }
    }
    return content;
  }

  private static final String GET_FOLDER_BY_PARENT_ID =
      "SELECT id, name, parentid, entityid, path FROM folder WHERE parentid = ?";
  private static final String GET_FOLDER_BY_PARENT_ID_INCLUDING_PARENT =
      "SELECT id, name, parentid, entityid, path FROM folder WHERE parentid = ? OR id = ?";

  public List<Folder> getFolderByParentId(
      Long parentId, boolean includeParentFolder, Connection connection) throws SQLException {
    List<Folder> folderList = new ArrayList<>();
    try (PreparedStatement statement =
        connection.prepareStatement(
            includeParentFolder
                ? GET_FOLDER_BY_PARENT_ID_INCLUDING_PARENT
                : GET_FOLDER_BY_PARENT_ID)) {
      statement.setLong(1, parentId);
      if (includeParentFolder) statement.setLong(2, parentId);
      try (final ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
          final Folder folder = resultSetToFolderMapper(rs);
          folderList.add(folder);
        }
      }
    }
    return folderList;
  }

  public List<Folder> getRecentlyVisited(String userId, int limit, Connection connection)
      throws SQLException {
    String sql =
        "SELECT f.id, f.name, f.type, f.path, f.pathhash, f.parentpathhash, f.contentid, f.createdtime, "
            + "f.createdby, f.updatedtime, f.updatedby, fa.lastvisitedtime from folder f "
            + "INNER JOIN folder_activity fa ON f.id = fa.folderid where fa.userid = ? "
            + "ORDER BY fa.lastvisitedtime desc limit ?";
    List<Folder> folders = new ArrayList();
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, userId);
      statement.setInt(2, limit);
      try (final ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
          Folder folder = resultSetToFolderMapper(rs);
          folder.setLastVisitedTime(rs.getTimestamp("lastvisitedtime"));
          folders.add(folder);
        }
      }
    }
    return folders;
  }

  public List<Folder> getFavorites(final String userId, final Connection connection)
      throws SQLException {
    String sql =
        "SELECT f.id, f.name, f.type, f.path, f.pathhash, f.parentpathhash, f.contentid, f.createdtime, "
            + "f.createdby, f.updatedtime, f.updatedby, ff.createdtime as favoritedtime from folder f "
            + "INNER JOIN favorite_folder ff ON f.id = ff.folderid where ff.userid = ?";
    List<Folder> folders = new ArrayList();
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, userId);
      try (final ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
          Folder folder = resultSetToFolderMapper(rs);
          folder.setFavoritedTime(rs.getTimestamp("favoritedtime"));
          folders.add(folder);
        }
      }
    }
    return folders;
  }

  public int addToFavorites(final long id, final String userId, final Connection connection)
      throws SQLException {
    String sql = "INSERT INTO favorite_folder (userid, folderid, createdtime) VALUES (?, ?, ?)";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, userId);
      statement.setLong(2, id);
      statement.setTimestamp(3, BaseService.now());
      return statement.executeUpdate();
    }
  }

  public int deleteFromFavorites(final String userId, final long id, final Connection connection)
      throws SQLException {
    String sql = "DELETE FROM favorite_folder WHERE userid = ? AND folderid = ?";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, userId);
      statement.setLong(2, id);
      return statement.executeUpdate();
    }
  }

  public int addActivity(final long id, final String userId, final Connection connection)
      throws SQLException {
    String sql =
        "INSERT INTO folder_activity (userid, folderid, lastvisitedtime) values (?, ?, ?) "
            + "ON DUPLICATE KEY UPDATE lastvisitedtime = ?";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      Timestamp now = BaseService.now();
      statement.setString(1, userId);
      statement.setLong(2, id);
      statement.setTimestamp(3, now);
      statement.setTimestamp(4, now);
      return statement.executeUpdate();
    }
  }

  public boolean isFavorite(String userId, long folderId, Connection connection)
      throws SQLException {
    String sql = "SELECT id from favorite_folder where userid = ? and folderid = ?";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, userId);
      statement.setLong(2, folderId);
      try (final ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
          return true;
        }
      }
    }
    return false;
  }
}
