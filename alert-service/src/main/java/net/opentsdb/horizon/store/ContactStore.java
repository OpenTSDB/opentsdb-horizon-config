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

package net.opentsdb.horizon.store;

import net.opentsdb.horizon.model.Contact;
import net.opentsdb.horizon.model.ContactType;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static net.opentsdb.horizon.util.Utils.deSerialize;
import static net.opentsdb.horizon.util.Utils.serialize;

public class ContactStore extends BaseStore {

  public ContactStore(DataSource rwSrc, DataSource roSrc) {
    super(rwSrc, roSrc);
  }

  private static final String SQL_ADD_CONTACT =
      "INSERT INTO contact(name, type, content, namespaceid, createdby, createdtime, updatedby, updatedtime) "
          + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

  public int[] createContact(
      final int namespaceId, final List<Contact> contacts, Connection connection)
      throws SQLException, IOException {

    try (PreparedStatement statement =
        connection.prepareStatement(SQL_ADD_CONTACT, Statement.RETURN_GENERATED_KEYS)) {
      for (Contact contact : contacts) {
        statement.setString(1, contact.getName());
        statement.setByte(2, contact.getType().getId());
        statement.setBytes(3, serialize(contact.getDetails()).getBytes());
        statement.setInt(4, namespaceId);
        statement.setString(5, contact.getCreatedBy());
        statement.setTimestamp(6, contact.getCreatedTime());
        statement.setString(7, contact.getUpdatedBy());
        statement.setTimestamp(8, contact.getUpdatedTime());

        statement.addBatch();
      }
      int[] result = statement.executeBatch();
      if (result.length > 0) {
        final ResultSet generatedKeys = statement.getGeneratedKeys();
        int i = 0;
        while (generatedKeys.next()) {
          final int id = generatedKeys.getInt(1);
          contacts.get(i++).setId(id);
        }
      }
      return result;
    }
  }

  public int createContact(final int namespaceId, final Contact contact, Connection connection)
      throws SQLException, IOException {

    try (PreparedStatement statement =
        connection.prepareStatement(SQL_ADD_CONTACT, Statement.RETURN_GENERATED_KEYS)) {
      statement.setString(1, contact.getName());
      statement.setByte(2, contact.getType().getId());
      statement.setInt(3, namespaceId);
      statement.setString(4, contact.getCreatedBy());
      statement.setTimestamp(5, contact.getCreatedTime());
      statement.setBytes(6, serialize(contact.getDetails()).getBytes());

      int result = statement.executeUpdate();
      final ResultSet generatedKeys = statement.getGeneratedKeys();
      while (generatedKeys.next()) {
        final int keys = generatedKeys.getInt(1);
        contact.setId(keys);
      }
      return result;
    }
  }

  private static final String SQL_GET_CONTACT_BY_TYPE =
      "SELECT * FROM contact WHERE namespaceid = ? AND type = ?";

  public List<Contact> getContactByType(
      final int namespaceId, final ContactType type, Connection connection)
      throws SQLException, IOException {
    List<Contact> contacts = new ArrayList<>();
    try (PreparedStatement statement = connection.prepareStatement(SQL_GET_CONTACT_BY_TYPE)) {
      statement.setInt(1, namespaceId);
      statement.setByte(2, type.getId());
      final ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        contacts.add(resultSetToContact(resultSet));
      }
    }
    return contacts;
  }

  private static final String SQL_GET_CONTACT_BY_Id = "SELECT * FROM contact WHERE id = ?";

  public Contact getContactById(final int id, Connection connection)
      throws SQLException, IOException {
    Contact contact = null;
    try (PreparedStatement statement = connection.prepareStatement(SQL_GET_CONTACT_BY_Id)) {
      statement.setInt(1, id);

      final ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        contact = resultSetToContact(resultSet);
      }
    }
    return contact;
  }

  private static final String SQL_GET_CONTACT_BY_TYPE_AND_NAME =
      "SELECT * FROM contact WHERE namespaceid = ? AND type = ? AND name = ?";

  public Contact getContactByTypeAndName(
      final int namespaceId, final ContactType type, final String name, Connection connection)
      throws SQLException, IOException {
    Contact contact = null;
    try (PreparedStatement statement =
        connection.prepareStatement(SQL_GET_CONTACT_BY_TYPE_AND_NAME)) {
      statement.setInt(1, namespaceId);
      statement.setByte(2, type.getId());
      statement.setString(3, name);

      final ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        contact = resultSetToContact(resultSet);
      }
    }
    return contact;
  }

  private static final String SQL_GET_CONTACT = "SELECT * FROM contact WHERE namespaceid = ?";

  public List<Contact> getContactsByNamespace(final int namespaceId, Connection connection)
      throws SQLException, IOException {
    List<Contact> contacts = new ArrayList<>();
    try (PreparedStatement statement = connection.prepareStatement(SQL_GET_CONTACT)) {
      statement.setInt(1, namespaceId);
      final ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        contacts.add(resultSetToContact(resultSet));
      }
    }
    return contacts;
  }

  private static final String SQL_UPDATE_CONTACT =
      "UPDATE contact SET name = ?, content = ?, updatedby = ?, updatedtime = ? WHERE id = ?";

  public int[] update(final List<Contact> contacts, Connection connection)
      throws SQLException, IOException {
    try (PreparedStatement statement = connection.prepareStatement(SQL_UPDATE_CONTACT)) {
      for (Contact contact : contacts) {
        statement.setString(1, contact.getName());
        statement.setBytes(2, serialize(contact.getDetails()).getBytes());
        statement.setString(3, contact.getUpdatedBy());
        statement.setTimestamp(4, contact.getUpdatedTime());

        statement.setInt(5, contact.getId());
        statement.addBatch();
      }

      return statement.executeBatch();
    }
  }

  private static final String SQL_DELETE_CONTACT =
      "DELETE FROM contact WHERE namespaceid = ? AND type = ? AND name = ?";

  public int[] delete(final List<Contact> contacts, Connection connection) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(SQL_DELETE_CONTACT)) {
      for (Contact contact : contacts) {
        statement.setInt(1, contact.getNamespaceid());
        statement.setByte(2, contact.getType().getId());
        statement.setString(3, contact.getName());
        statement.addBatch();
      }

      return statement.executeBatch();
    }
  }

  private static final String SQL_DELETE_CONTACT_BY_ID = "DELETE FROM contact WHERE id = ?";

  public int[] deleteByIds(List<Integer> contactIds, Connection connection) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(SQL_DELETE_CONTACT_BY_ID)) {
      for (int id : contactIds) {
        statement.setInt(1, id);
        statement.addBatch();
      }
      return statement.executeBatch();
    }
  }

  public static final String SQL_DELETE_ALERT_CONTACT_BY_ALERT =
      "DELETE FROM alert_contact WHERE alertid = ?";

  public int deleteAlertContactByAlert(long alertid, Connection connection) throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(SQL_DELETE_ALERT_CONTACT_BY_ALERT)) {
      statement.setLong(1, alertid);
      int result = statement.executeUpdate();
      return result;
    }
  }

  public int[] deleteAlertContactsByAlert(List<Long> ids, Connection connection)
      throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(SQL_DELETE_ALERT_CONTACT_BY_ALERT)) {
      for (Long id : ids) {
        statement.setLong(1, id);
        statement.addBatch();
      }
      return statement.executeBatch();
    }
  }

  public static final String SQL_DELETE_ALERT_CONTACT_BY_CONTACT =
      "DELETE FROM alert_contact WHERE contactid = ?";

  public int[] deleteAlertContactsByContact(List<Integer> ids, Connection connection)
      throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(SQL_DELETE_ALERT_CONTACT_BY_CONTACT)) {
      for (Integer id : ids) {
        statement.setLong(1, id);
        statement.addBatch();
      }
      return statement.executeBatch();
    }
  }

  public static final String SQL_GET_CONTACT_FOR_ALERT =
      "SELECT contact.id AS id, contact.name AS name, contact"
          + ".type AS type, contact.namespaceid AS namespaceid, contact.content AS content, contact.createdby AS "
          + "createdby, contact.createdtime AS createdtime, contact.updatedby AS updatedby, contact.updatedtime as "
          + "updatedtime FROM contact JOIN alert_contact ON contact.id = alert_contact.contactid AND alert_contact"
          + ".alertid = ?";

  public List<Contact> getContactsForAlert(long alertId, Connection connection)
      throws SQLException, IOException {
    List<Contact> contacts = new ArrayList<>();
    try (PreparedStatement statement = connection.prepareStatement(SQL_GET_CONTACT_FOR_ALERT)) {
      statement.setLong(1, alertId);
      final ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        contacts.add(resultSetToContact(resultSet));
      }
    }
    return contacts;
  }

  private Contact resultSetToContact(ResultSet resultSet) throws SQLException, IOException {
    Contact contact = new Contact();
    contact.setId(resultSet.getInt("id"));
    contact.setNamespaceid(resultSet.getInt("namespaceid"));
    contact.setName(resultSet.getString("name"));
    contact.setType(ContactType.getById(resultSet.getByte("type")));
    contact.setDetails(deSerialize(resultSet.getBytes("content"), Map.class));
    contact.setCreatedBy(resultSet.getString("createdby"));
    contact.setCreatedTime(resultSet.getTimestamp("createdtime"));
    contact.setUpdatedBy(resultSet.getString("updatedby"));
    contact.setUpdatedTime(resultSet.getTimestamp("updatedtime"));

    return contact;
  }
}
