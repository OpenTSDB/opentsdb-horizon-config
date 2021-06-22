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

package net.opentsdb.horizon.util;

import net.opentsdb.horizon.fs.model.Content;
import net.opentsdb.horizon.fs.model.File;
import net.opentsdb.horizon.fs.model.FileHistory;
import net.opentsdb.horizon.fs.model.Folder;
import net.opentsdb.horizon.fs.view.FolderType;
import net.opentsdb.horizon.model.Alert;
import net.opentsdb.horizon.model.Contact;
import net.opentsdb.horizon.model.ContactType;
import net.opentsdb.horizon.model.Favorite;
import net.opentsdb.horizon.model.FolderActivity;
import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.model.Snooze;
import net.opentsdb.horizon.model.User;
import net.opentsdb.horizon.model.User.CreationMode;
import net.opentsdb.horizon.store.AlertStore;
import net.opentsdb.horizon.store.ResultSetMapper;
import net.opentsdb.horizon.store.SnoozeStore;
import net.opentsdb.horizon.model.Activity;
import net.opentsdb.horizon.model.ContentHistory;
import net.opentsdb.horizon.model.Snapshot;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.mapper.reflect.FieldMapper;
import org.jdbi.v3.core.statement.PreparedBatch;
import org.jdbi.v3.core.statement.StatementContext;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static net.opentsdb.horizon.util.Utils.deSerialize;
import static net.opentsdb.horizon.util.Utils.serialize;

public class DBUtil {

  private Jdbi jdbi;

  public DBUtil(DataSource dataSource) {
    this.jdbi = Jdbi.create(dataSource);
    this.jdbi.registerColumnMapper(
        CreationMode.class, (rs, col, ctx) -> CreationMode.values()[rs.getInt(col)]);
  }

  public void insert(User user) {
    final String sql =
        "INSERT INTO user (userid, name, enabled, creationmode, updatedtime) VALUES (?, ?, ?, ?, ?)";
    jdbi.useHandle(
        handle ->
            handle
                .createUpdate(sql)
                .bind(0, user.getUserid())
                .bind(1, user.getName())
                .bind(2, user.isEnabled())
                .bind(3, user.getCreationmode().id)
                .bind(4, user.getUpdatedtime())
                .execute());
  }

  public User getUserById(final String id) {
    final String sql = "SELECT * FROM user WHERE userid = ?";
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(sql)
                .bind(0, id)
                .registerRowMapper(FieldMapper.factory(User.class))
                .mapTo(User.class)
                .one());
  }

  public Namespace getNamespaceByName(final String name) {
    final String sql = "SELECT * FROM namespace WHERE name = ?";
    return jdbi.withHandle(
        handle -> handle.createQuery(sql).bind(0, name).map(new NamespaceMapper()).one());
  }

  public Namespace getNamespaceById(final int namespaceId) {
    final String sql = "SELECT * FROM namespace WHERE id = ?";
    Optional<Namespace> namespace =
        jdbi.withHandle(
            handle ->
                handle.createQuery(sql).bind(0, namespaceId).map(new NamespaceMapper()).findOne());
    return namespace.get();
  }

  public Alert getAlert(final long alertId) {
    final String sql = "SELECT * FROM alert WHERE id = ?";
    Optional<Alert> alert =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery(sql)
                    .bind(0, alertId)
                    .map(
                        (rs, ctx) -> {
                          try {
                            return AlertStore.resultSetToAlert(rs, true);
                          } catch (IOException e) {
                            throw new RuntimeException("Error parsing resultset", e);
                          }
                        })
                    .findOne());
    return alert.get();
  }

  public int insert(Namespace namespace) throws IOException {
    final String sql =
        "INSERT INTO namespace (name, alias, meta, enabled, createdby, createdtime, updatedby, updatedtime) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    final int namespaceId =
        jdbi.withHandle(
            handle ->
                handle
                    .createUpdate(sql)
                    .bind(0, namespace.getName())
                    .bind(1, namespace.getAlias())
                    .bind(2, serialize(namespace.getMeta()))
                    .bind(3, namespace.getEnabled())
                    .bind(4, namespace.getCreatedBy())
                    .bind(5, namespace.getCreatedTime())
                    .bind(6, namespace.getUpdatedBy())
                    .bind(7, namespace.getUpdatedTime())
                    .executeAndReturnGeneratedKeys("id")
                    .mapTo(Integer.class)
                    .one());
    return namespaceId;
  }

  public void insert(Content content) {
    final String sql =
        "INSERT IGNORE INTO content (sha2, data, createdtime, createdby) VALUES (?, ?, ?, ?)";
    jdbi.useHandle(
        handle ->
            handle
                .createUpdate(sql)
                .bind(0, content.getSha2())
                .bind(1, content.getData())
                .bind(2, content.getCreatedtime())
                .bind(3, content.getCreatedby())
                .execute());
  }

  public Folder getFolderByName(final String name) {
    return getFolder("SELECT * FROM folder WHERE name = ?", name);
  }

  public Folder getFolderById(long actualId) {
    return getFolder("SELECT * FROM folder WHERE id = ?", actualId);
  }

  private Folder getFolder(String sql, Object binder) {
    Optional<Folder> folder =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery(sql)
                    .bind(0, binder)
                    .map(
                        (rs, ctx) -> {
                          Folder f = new File();
                          f.setId(rs.getLong("id"));
                          f.setName(rs.getString("name"));
                          f.setType(FolderType.values()[rs.getInt("type")]);
                          f.setPath(rs.getString("path"));
                          f.setPathHash(rs.getBytes("pathhash"));
                          f.setParentPathHash(rs.getBytes("parentpathhash"));
                          f.setContentid(rs.getBytes("contentid"));
                          f.setCreatedTime(rs.getTimestamp("createdtime"));
                          f.setCreatedBy(rs.getString("createdby"));
                          f.setUpdatedTime(rs.getTimestamp("updatedtime"));
                          f.setUpdatedBy(rs.getString("updatedby"));
                          return f;
                        })
                    .findOne());
    return folder.get();
  }

  public Content getContentById(byte[] contentId) {
    String sql = "SELECT * FROM content WHERE sha2 = ?";
    Optional<Content> content =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery(sql)
                    .bind(0, contentId)
                    .map(
                        (rs, ctx) -> {
                          Content c = new Content();
                          c.setSha2(rs.getBytes("sha2"));
                          c.setData(rs.getBytes("data"));
                          c.setCreatedby(rs.getString("createdby"));
                          c.setCreatedtime(rs.getTimestamp("createdtime"));
                          return c;
                        })
                    .findOne());
    return content.get();
  }

  public FileHistory getHistoryByFileId(long folderId) {
    String sql = "SELECT * FROM folder_history WHERE folderid = ?";
    Optional<FileHistory> fileHistory =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery(sql)
                    .bind(0, folderId)
                    .map(
                        (rs, ctx) -> {
                          FileHistory fh = new FileHistory();
                          fh.setId(rs.getLong("id"));
                          fh.setFileid(rs.getLong("folderid"));
                          fh.setContentid(rs.getBytes("contentid"));
                          fh.setCreatedtime(rs.getTimestamp("createdtime"));
                          return fh;
                        })
                    .findOne());
    return fileHistory.get();
  }

  public Optional<Favorite> getFavoriteFolder(final long folderId) {
    String sql = "SELECT * FROM favorite_folder WHERE folderid = ?";
    Optional<Favorite> favorite =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery(sql)
                    .bind(0, folderId)
                    .map(
                        (rs, ctx) -> {
                          Favorite f = new Favorite();
                          f.setId(rs.getLong("id"));
                          f.setUserId(rs.getString("userid"));
                          f.setFolderId(rs.getLong("folderid"));
                          f.setCreatedTime(rs.getTimestamp("createdtime"));
                          return f;
                        })
                    .findOne());
    return favorite;
  }

  public long insert(Favorite favorite) {
    final String sql =
        "INSERT INTO favorite_folder (userid, folderid, createdtime) VALUES (?, ?, ?)";
    return jdbi.withHandle(
        handle ->
            handle
                .createUpdate(sql)
                .bind(0, favorite.getUserId())
                .bind(1, favorite.getFolderId())
                .bind(2, favorite.getCreatedTime())
                .executeAndReturnGeneratedKeys("id")
                .mapTo(Long.class)
                .one());
  }

  public FolderActivity getFolderActivity(String userId, long folderId) {

    String sql = "SELECT lastvisitedtime FROM folder_activity WHERE userid = ? AND folderid = ?";
    Optional<FolderActivity> favorite =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery(sql)
                    .bind(0, userId)
                    .bind(1, folderId)
                    .map(
                        (rs, ctx) -> {
                          FolderActivity f = new FolderActivity();
                          f.setUserId(userId);
                          f.setFolderId(folderId);
                          f.setLastVisitedTime(rs.getTimestamp("lastvisitedtime"));
                          return f;
                        })
                    .findOne());
    return favorite.get();
  }

  public void insert(FolderActivity folderActivity) {
    final String sql =
        "INSERT INTO folder_activity (userid, folderid, lastvisitedtime) VALUES (?, ?, ?)";
    jdbi.withHandle(
        handle ->
            handle
                .createUpdate(sql)
                .bind(0, folderActivity.getUserId())
                .bind(1, folderActivity.getFolderId())
                .bind(2, folderActivity.getLastVisitedTime())
                .execute());
  }

  public int insert(Contact contact) throws IOException {
    final String sql =
        "INSERT INTO contact(name, type, content, namespaceid, createdby, createdtime, updatedby, updatedtime) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    return jdbi.withHandle(
        handle ->
            handle
                .createUpdate(sql)
                .bind(0, contact.getName())
                .bind(1, contact.getType().getId())
                .bind(2, serialize(contact.getDetails()).getBytes())
                .bind(3, contact.getNamespaceid())
                .bind(4, contact.getCreatedBy())
                .bind(5, contact.getCreatedTime())
                .bind(6, contact.getUpdatedBy())
                .bind(7, contact.getUpdatedTime())
                .executeAndReturnGeneratedKeys("id")
                .mapTo(Integer.class)
                .one());
  }

  public int[] insertAlertContact(long alertId, List<Integer> contactIds) {
    final String sql = "INSERT INTO alert_contact(alertid, contactid) VALUES (?, ?)";
    return jdbi.withHandle(
        handle -> {
          PreparedBatch batch = handle.prepareBatch(sql);
          for (int contactId : contactIds) {
            batch.bind(0, alertId);
            batch.bind(1, contactId);
          }
          return batch.execute();
        });
  }

  public Snapshot getSnapshot(long id) {
    final String sql = "SELECT * FROM snapshot WHERE id = ?";

    return jdbi.withHandle(
            handle -> handle.createQuery(sql).bind(0, id).map(new SnapshotMapper()).findOne())
        .get();
  }

  public List<ContentHistory> getContentHistory(byte contentType, long entityId) {
    final String sql = "SELECT * FROM content_history WHERE contenttype = ? AND entityid = ?";
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(sql)
                .bind(0, contentType)
                .bind(1, entityId)
                .map(new ContentHistoryMapper())
                .list());
  }

  public void insert(Snapshot snapshot) {
    final String sql =
        "INSERT INTO snapshot(name, sourcetype, sourceid, contentid, createdby, createdtime, updatedby, updatedtime) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    Long id =
        jdbi.withHandle(
            handle ->
                handle
                    .createUpdate(sql)
                    .bind(0, snapshot.getName())
                    .bind(1, snapshot.getSourceType())
                    .bind(2, snapshot.getSourceId())
                    .bind(3, snapshot.getContentId())
                    .bind(4, snapshot.getCreatedBy())
                    .bind(5, snapshot.getCreatedTime())
                    .bind(6, snapshot.getUpdatedBy())
                    .bind(7, snapshot.getUpdatedTime())
                    .executeAndReturnGeneratedKeys("id")
                    .mapTo(Long.class)
                    .one());
    snapshot.setId(id);
  }

  public void insert(net.opentsdb.horizon.model.Content content) {
    Content model = new Content();
    model.setSha2(content.getSha2());
    model.setData(content.getData());
    model.setCreatedby(content.getCreatedBy());
    model.setCreatedtime(content.getCreatedTime());
    insert(model);
  }

  public void insert(ContentHistory history) {
    final String sql =
        "INSERT IGNORE INTO content_history(contenttype, entityid, contentid, createdby, createdtime) VALUES (?, ?, ?, ?, ?)";
    Long id =
        jdbi.withHandle(
            handle ->
                handle
                    .createUpdate(sql)
                    .bind(0, history.getContentType())
                    .bind(1, history.getEntityId())
                    .bind(2, history.getContentId())
                    .bind(3, history.getCreatedBy())
                    .bind(4, history.getCreatedTime())
                    .executeAndReturnGeneratedKeys("id")
                    .mapTo(Long.class)
                    .one());
    history.setId(id);
  }

  public int getRecordCount(String table) {
    final String sql = "SELECT COUNT(*) as count FROM " + table;
    return jdbi.withHandle(handle -> handle.createQuery(sql).mapTo(Integer.class).one());
  }

  public int getRecordCount(String table, String condition) {
    final String sql = "SELECT COUNT(*) as count FROM " + table + " WHERE " + condition;
    return jdbi.withHandle(handle -> handle.createQuery(sql).mapTo(Integer.class).one());
  }

  public Activity getActivity(String userId, byte entityType, long entityId) {
    String sql =
        "SELECT timestamp FROM activity WHERE userid = ? AND entitytype = ? AND entityid = ?";
    Optional<Activity> favorite =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery(sql)
                    .bind(0, userId)
                    .bind(1, entityType)
                    .bind(2, entityId)
                    .map(
                        (rs, ctx) -> {
                          Activity f = new Activity();
                          f.setUserId(userId);
                          f.setEntityType(entityType);
                          f.setEntityId(entityId);
                          f.setTimestamp(rs.getTimestamp("timestamp"));
                          return f;
                        })
                    .findOne());
    return favorite.get();
  }

  public void insert(Activity activity) {
    String sql =
        "INSERT INTO activity (userid, entitytype, entityid, timestamp) values (?, ?, ?, ?) "
            + "ON DUPLICATE KEY UPDATE timestamp = ?";
    jdbi.withHandle(
        handle ->
            handle
                .createUpdate(sql)
                .bind(0, activity.getUserId())
                .bind(1, activity.getEntityType())
                .bind(2, activity.getEntityId())
                .bind(3, activity.getTimestamp())
                .bind(4, activity.getTimestamp())
                .execute());
  }

  public Optional<Snooze> getSnooze(final long id) {
    final String sql = "SELECT * FROM snooze WHERE id = ?";
    Optional<Snooze> snooze =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery(sql)
                    .bind(0, id)
                    .map(
                        (rs, ctx) -> {
                          try {
                            return SnoozeStore.resultSetToSnooze(rs);
                          } catch (IOException e) {
                            throw new RuntimeException("Error parsing resultset", e);
                          }
                        })
                    .findOne());
    return snooze;
  }

  class ContentHistoryMapper implements RowMapper<ContentHistory> {

    @Override
    public ContentHistory map(ResultSet rs, StatementContext ctx) throws SQLException {
      ContentHistory history = new ContentHistory();
      history.setId(rs.getLong("id"));
      history.setContentType(rs.getByte("contenttype"));
      history.setEntityId(rs.getLong("entityid"));
      history.setContentId(rs.getBytes("contentid"));
      history.setCreatedBy(rs.getString("createdby"));
      history.setCreatedTime(rs.getTimestamp("createdtime"));
      return history;
    }
  }

  class SnapshotMapper implements RowMapper<Snapshot> {

    @Override
    public Snapshot map(ResultSet rs, StatementContext ctx) throws SQLException {
      Snapshot snapshot = new Snapshot();
      snapshot.setId(rs.getLong("id"));
      snapshot.setName(rs.getString("name"));
      snapshot.setSourceType(rs.getByte("sourcetype"));
      snapshot.setSourceId(rs.getLong("sourceid"));
      snapshot.setContentId(rs.getBytes("contentid"));
      snapshot.setCreatedBy(rs.getString("createdby"));
      snapshot.setCreatedTime(rs.getTimestamp("createdtime"));
      snapshot.setUpdatedBy(rs.getString("updatedby"));
      snapshot.setUpdatedTime(rs.getTimestamp("updatedtime"));
      return snapshot;
    }
  }

  class NamespaceMapper implements RowMapper<Namespace> {
    @Override
    public Namespace map(ResultSet rs, StatementContext ctx) throws SQLException {
      try {
        return ResultSetMapper.resultSetToNamespace(rs);
      } catch (IOException e) {
        throw new SQLException(e);
      }
    }
  }

  class ContactMapper implements RowMapper<Contact> {
    @Override
    public Contact map(ResultSet rs, StatementContext ctx) throws SQLException {
      try {
        Contact contact = new Contact();
        contact.setId(rs.getInt("id"));
        contact.setNamespaceid(rs.getInt("namespaceid"));
        contact.setName(rs.getString("name"));
        contact.setType(ContactType.getById(rs.getByte("type")));
        contact.setDetails(deSerialize(rs.getBytes("content"), Map.class));
        contact.setCreatedBy(rs.getString("createdby"));
        contact.setCreatedTime(rs.getTimestamp("createdtime"));
        contact.setUpdatedBy(rs.getString("updatedby"));
        contact.setUpdatedTime(rs.getTimestamp("updatedtime"));
        return contact;
      } catch (IOException e) {
        throw new SQLException(e);
      }
    }
  }

  public int getAllContentCount() {
    final String sql = "select count(*) from content";
    return jdbi.withHandle(handle -> handle.createQuery(sql).mapTo(Integer.class).one());
  }

  public int getAllFolderCount() {
    final String sql = "select count(*) from folder";
    return jdbi.withHandle(handle -> handle.createQuery(sql).mapTo(Integer.class).one());
  }

  public long insert(Folder folder) {
    final String sql =
        "INSERT INTO folder(name, type, path, pathhash, parentpathhash, contentid, createdtime, createdby, updatedtime, updatedby) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    final long id =
        jdbi.withHandle(
            handle ->
                handle
                    .createUpdate(sql)
                    .bind(0, folder.getName())
                    .bind(1, folder.getType().value)
                    .bind(2, folder.getPath())
                    .bind(3, folder.getPathHash())
                    .bind(4, folder.getParentPathHash())
                    .bind(5, folder.getContentid())
                    .bind(6, folder.getCreatedTime())
                    .bind(7, folder.getCreatedBy())
                    .bind(8, folder.getUpdatedTime())
                    .bind(9, folder.getUpdatedBy())
                    .executeAndReturnGeneratedKeys("id")
                    .mapTo(Long.class)
                    .one());
    return id;
  }

  public void insert(FileHistory fileHistory) {
    final String sql =
        "INSERT IGNORE INTO folder_history(folderid, contentid, createdtime) VALUES (?, ?, ?)";
    jdbi.useHandle(
        handle ->
            handle
                .createUpdate(sql)
                .bind(0, fileHistory.getFileid())
                .bind(1, fileHistory.getContentid())
                .bind(2, fileHistory.getCreatedtime())
                .execute());
  }

  public List<String> getNamespaceMember(int namespaceid) {
    final String sql = "SELECT userid FROM namespace_member WHERE namespaceid = ?";
    return jdbi.withHandle(
        handle -> handle.createQuery(sql).bind(0, namespaceid).mapTo(String.class).list());
  }

  public void insertNamespaceMember(int namespaceid, String userid) {
    final String sql = "INSERT INTO namespace_member (namespaceid, userid) VALUES (?, ?)";
    jdbi.useHandle(
        handle -> handle.createUpdate(sql).bind(0, namespaceid).bind(1, userid).execute());
  }

  public List<String> getNamespaceFollower(int namespaceid) {
    final String sql = "SELECT userid FROM namespace_follower WHERE namespaceid = ?";
    return jdbi.withHandle(
        handle -> handle.createQuery(sql).bind(0, namespaceid).mapTo(String.class).list());
  }

  public void insertNamespaceFollower(int namespaceid, String userid) {
    final String sql = "INSERT INTO namespace_follower (namespaceid, userid) VALUES (?, ?)";
    jdbi.useHandle(
        handle -> handle.createUpdate(sql).bind(0, namespaceid).bind(1, userid).execute());
  }

  public void insertBcpService(
      String serviceName, String primaryColo, Timestamp failoverTime, long delay) {
    final String meta = String.format("{ \"coloList\": [\"den\", \"lga\"], \"delay\": %d}", delay);
    final String sql =
        "INSERT INTO bcp (serviceName, primaryColo, failoverTime, meta) VALUES (?, ?, ?, ?)";
    jdbi.useHandle(
        handle ->
            handle
                .createUpdate(sql)
                .bind(0, serviceName)
                .bind(1, primaryColo)
                .bind(2, failoverTime)
                .bind(3, meta)
                .execute());
  }

  public void clearTable(String table) {
    String sql = String.format("DELETE FROM " + table);
    execute(sql);
  }

  public void execute(String sql, Object... args) {
    jdbi.useHandle(handle -> handle.execute(sql, args));
  }

  public int createContact(Contact contact) throws IOException {
    final String sql =
        "INSERT INTO contact(name, type, namespaceid, createdby, createdtime, content) VALUES (?, ?, ?, ?, ?, ?)";
    final int entityId =
        jdbi.withHandle(
            handle ->
                handle
                    .createUpdate(sql)
                    .bind(0, contact.getName())
                    .bind(1, contact.getType().getId())
                    .bind(2, contact.getNamespaceid())
                    .bind(3, contact.getCreatedBy())
                    .bind(4, contact.getCreatedTime())
                    .bind(5, serialize(contact.getDetails()))
                    .executeAndReturnGeneratedKeys("id")
                    .mapTo(Integer.class)
                    .one());
    return entityId;
  }

  public List<Contact> getContactForNamespace(final int namespaceId) {
    final String sql = "SELECT * FROM contact WHERE namespaceid = ?";
    return jdbi.withHandle(
        handle -> handle.createQuery(sql).bind(0, namespaceId).map(new ContactMapper()).list());
  }

  public List<Contact> getAlertContacts(final long alertId) {
    final String sql =
        "SELECT contact.id AS id, contact.name AS name, contact"
            + ".type AS type, contact.namespaceid AS namespaceid, contact.content AS content, contact.createdby AS "
            + "createdby, contact.createdtime AS createdtime, contact.updatedby AS updatedby, contact.updatedtime as "
            + "updatedtime FROM contact JOIN alert_contact ON contact.id = alert_contact.contactid AND alert_contact"
            + ".alertid = ?";

    return jdbi.withHandle(
        handle -> handle.createQuery(sql).bind(0, alertId).map(new ContactMapper()).list());
  }

  public List<Integer> getContactIdsForAlert(final long alertId) {
    final String sql = "SELECT contactid FROM alert_contact WHERE alertid = ?";
    return jdbi.withHandle(
        handle -> handle.createQuery(sql).bind(0, alertId).mapTo(Integer.class).list());
  }

  public Optional<Contact> getContactById(int id) {
    final String sql = "SELECT * FROM contact WHERE id = ?";
    return jdbi.withHandle(
        handle -> handle.createQuery(sql).bind(0, id).map(new ContactMapper()).findOne());
  }

  public long createAlert(Alert alert) throws IOException {
    final String sql =
        "INSERT INTO alert(name, type, labels, definition, enabled, deleted, namespaceid, createdby, createdtime, "
            + "updatedby, updatedtime) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    final long entityId =
        jdbi.withHandle(
            handle ->
                handle
                    .createUpdate(sql)
                    .bind(0, alert.getName())
                    .bind(1, alert.getType().getId())
                    .bind(2, serialize(alert.getLabels()))
                    .bind(3, serialize(alert.getDefinition()))
                    .bind(4, alert.isEnabled() ? (byte) 1 : (byte) 0)
                    .bind(5, alert.isDeleted() ? (byte) 1 : (byte) 0)
                    .bind(6, alert.getNamespaceId())
                    .bind(7, alert.getCreatedBy())
                    .bind(8, alert.getCreatedTime())
                    .bind(9, alert.getUpdatedBy())
                    .bind(10, alert.getUpdatedTime())
                    .executeAndReturnGeneratedKeys("id")
                    .mapTo(Long.class)
                    .one());
    return entityId;
  }

  public long create(Snooze snooze) throws IOException {
    final String sql =
        "INSERT INTO snooze(definition, starttime, endtime, enabled, deleted, namespaceid, "
            + "createdby, createdtime,updatedby, updatedtime) "
            + "VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    final long entityId =
        jdbi.withHandle(
            handle ->
                handle
                    .createUpdate(sql)
                    .bind(0, serialize(snooze.getDefinition()))
                    .bind(1, snooze.getStartTime())
                    .bind(2, snooze.getEndTime())
                    .bind(3, snooze.isEnabled() ? (byte) 1 : (byte) 0)
                    .bind(4, snooze.isDeleted() ? (byte) 1 : (byte) 0)
                    .bind(5, snooze.getNamespaceId())
                    .bind(6, snooze.getCreatedBy())
                    .bind(7, snooze.getCreatedTime())
                    .bind(8, snooze.getUpdatedBy())
                    .bind(9, snooze.getUpdatedTime())
                    .executeAndReturnGeneratedKeys("id")
                    .mapTo(Long.class)
                    .one());
    return entityId;
  }
}
