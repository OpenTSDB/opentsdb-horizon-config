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

package net.opentsdb.horizon.fs.model;

import net.opentsdb.horizon.fs.view.FolderType;
import net.opentsdb.horizon.view.BaseDto;

import javax.persistence.Transient;
import java.sql.Timestamp;

public class Folder extends BaseDto {

    private long id;
    private String name;
    private FolderType type;
    private String path;
    private byte[] pathHash;
    private byte[] parentPathHash;
    private byte[] contentid;

    /**
     * Read from folder activity for the get recent files and folders.
     */
    @Transient
    private Timestamp lastVisitedTime;

    /**
     * Read from favorite folder for the get favorite files and folders.
     */
    @Transient
    private Timestamp favoritedTime;

    @Transient
    private String slug;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public FolderType getType() {
        return type;
    }

    public void setType(FolderType type) {
        this.type = type;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public byte[] getPathHash() {
        return pathHash;
    }

    public void setPathHash(byte[] pathHash) {
        this.pathHash = pathHash;
    }

    public byte[] getParentPathHash() {
        return parentPathHash;
    }

    public void setParentPathHash(byte[] parentPathHash) {
        this.parentPathHash = parentPathHash;
    }

    public byte[] getContentid() {
        return contentid;
    }

    public void setContentid(byte[] contentid) {
        this.contentid = contentid;
    }

    public String getSlug() {
        return slug;
    }

    public void setSlug(String slug) {
        this.slug = slug;
    }

    public Timestamp getLastVisitedTime() {
        return lastVisitedTime;
    }

    public void setLastVisitedTime(Timestamp lastVisitedTime) {
        this.lastVisitedTime = lastVisitedTime;
    }

    public Timestamp getFavoritedTime() {
        return favoritedTime;
    }

    public void setFavoritedTime(Timestamp favoritedTime) {
        this.favoritedTime = favoritedTime;
    }

    public boolean isFile() {
        return null != contentid;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Folder)) {
            return false;
        }

        Folder that = (Folder) obj;
        return this.path.equals(that.path);
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }
}
