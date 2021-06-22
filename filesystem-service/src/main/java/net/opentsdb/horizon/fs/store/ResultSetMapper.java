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

import net.opentsdb.horizon.fs.model.Folder;
import net.opentsdb.horizon.fs.view.FolderType;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ResultSetMapper {

    public static Folder resultSetToFolderMapper(ResultSet resultSet) throws SQLException {
        Folder folder = new Folder();
        folder.setId(resultSet.getLong("id"));
        folder.setName(resultSet.getString("name"));
        folder.setType(FolderType.values()[resultSet.getByte("type")]);
        folder.setPath(resultSet.getString("path"));
        folder.setPathHash(resultSet.getBytes("pathhash"));
        folder.setParentPathHash(resultSet.getBytes("parentpathhash"));
        folder.setContentid(resultSet.getBytes("contentid"));
        folder.setCreatedTime(resultSet.getTimestamp("createdtime"));
        folder.setCreatedBy(resultSet.getString("createdby"));
        folder.setUpdatedTime(resultSet.getTimestamp("updatedtime"));
        folder.setUpdatedBy(resultSet.getString("updatedby"));
        return folder;
    }

}
