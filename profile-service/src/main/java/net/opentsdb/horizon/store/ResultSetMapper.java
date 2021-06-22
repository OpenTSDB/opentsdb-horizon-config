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


import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.model.User;
import net.opentsdb.horizon.model.User.CreationMode;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static net.opentsdb.horizon.util.Utils.deSerialize;

public class ResultSetMapper {

    public static List<User> resultSetToUserListMapper(ResultSet resultSet) throws SQLException {
        List<User> usersList = new ArrayList<>();
        while (resultSet.next()) {
            User user = new User();
            user.setUserid(resultSet.getString("userid"));
            user.setName(resultSet.getString("name"));
            user.setEnabled(resultSet.getBoolean("enabled"));
            usersList.add(user);
        }
        return usersList;
    }

    public static Namespace resultSetToNamespace(ResultSet resultSet) throws SQLException, IOException {
        Namespace namespace = new Namespace();
        namespace.setId(resultSet.getInt("id"));
        namespace.setName(resultSet.getString("name"));
        namespace.setAlias(resultSet.getString("alias"));
        namespace.setMeta(deSerialize(resultSet.getBytes("meta"), Map.class));
        namespace.setEnabled(resultSet.getBoolean("enabled"));
        namespace.setCreatedBy(resultSet.getString("createdby"));
        namespace.setCreatedTime(resultSet.getTimestamp("createdtime"));
        namespace.setUpdatedBy(resultSet.getString("updatedby"));
        namespace.setUpdatedTime(resultSet.getTimestamp("updatedtime"));

        return namespace;
    }

    public static User userMapper(final ResultSet rs) throws SQLException {
        User user = new User();
        user.setUserid(rs.getString("userid"));
        user.setName(rs.getString("name"));
        user.setEnabled(rs.getBoolean("enabled"));
        user.setCreationmode(CreationMode.values()[rs.getByte("creationmode")]);
        user.setUpdatedtime(rs.getTimestamp("updatedtime"));
        user.setDisabledtime(rs.getTimestamp("disabledtime"));
        return user;
    }
}
