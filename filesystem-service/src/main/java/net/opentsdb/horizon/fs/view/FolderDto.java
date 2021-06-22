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

package net.opentsdb.horizon.fs.view;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import net.opentsdb.horizon.view.BaseDto;

import java.sql.Timestamp;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class FolderDto extends BaseDto {

    private Long id;
    private String name;
    private FolderType type;
    private String path;
    private String fullPath;
    private Long parentId;
    private String parentPath;
    private Boolean favorite;
    private Timestamp favoritedTime;
    private Timestamp lastVisitedTime;

    private List<FolderDto> subfolders;
    private List<FolderDto> files;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getFullPath() {
        return fullPath;
    }

    public void setFullPath(String fullPath) {
        this.fullPath = fullPath;
    }

    public Long getParentId() {
        return parentId;
    }

    public void setParentId(Long parentId) {
        this.parentId = parentId;
    }

    public String getParentPath() {
        return parentPath;
    }

    public void setParentPath(String parentPath) {
        this.parentPath = parentPath;
    }

    public FolderType getType() {
        return type;
    }

    public void setType(FolderType type) {
        this.type = type;
    }

    public List<FolderDto> getSubfolders() {
        return subfolders;
    }

    public void setSubfolders(List<FolderDto> subfolders) {
        this.subfolders = subfolders;
    }

    public List<FolderDto> getFiles() {
        return files;
    }

    public void setFiles(List<FolderDto> files) {
        this.files = files;
    }

    public Boolean getFavorite() {
        return favorite;
    }

    public void setFavorite(Boolean favorite) {
        this.favorite = favorite;
    }

    public Timestamp getFavoritedTime() {
        return favoritedTime;
    }

    public void setFavoritedTime(Timestamp favoritedTime) {
        this.favoritedTime = favoritedTime;
    }

    public Timestamp getLastVisitedTime() {
        return lastVisitedTime;
    }

    public void setLastVisitedTime(Timestamp lastVisitedTime) {
        this.lastVisitedTime = lastVisitedTime;
    }
}
