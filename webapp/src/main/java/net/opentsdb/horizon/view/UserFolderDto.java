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

package net.opentsdb.horizon.view;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import net.opentsdb.horizon.fs.view.FolderDto;
import net.opentsdb.horizon.model.User;

import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class UserFolderDto {
    private User user;
    private FolderDto personalFolder;
    private List<NamespaceFolderDto> memberNamespaces;
    private List<NamespaceFolderDto> followerNamespaces;

    private List<FolderDto> favorites;
    private List<FolderDto> recent;

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public FolderDto getPersonalFolder() {
        return personalFolder;
    }

    public void setPersonalFolder(FolderDto personalFolder) {
        this.personalFolder = personalFolder;
    }

    public List<NamespaceFolderDto> getMemberNamespaces() {
        return memberNamespaces;
    }

    public void setMemberNamespaces(List<NamespaceFolderDto> memberNamespaces) {
        this.memberNamespaces = memberNamespaces;
    }

    public void addToMemberNamespaces(NamespaceFolderDto folderDto) {
        if (memberNamespaces == null) {
            memberNamespaces = new ArrayList<>();
        }
        memberNamespaces.add(folderDto);
    }

    public List<NamespaceFolderDto> getFollowerNamespaces() {
        return followerNamespaces;
    }

    public void setFollowerNamespaces(List<NamespaceFolderDto> followerNamespaces) {
        this.followerNamespaces = followerNamespaces;
    }

    public void addToFollowerNamespaces(NamespaceFolderDto folderDto) {
        if (followerNamespaces == null) {
            followerNamespaces = new ArrayList<>();
        }
        followerNamespaces.add(folderDto);
    }

    public List<FolderDto> getFavorites() {
        return favorites;
    }

    public void setFavorites(List<FolderDto> favorites) {
        this.favorites = favorites;
    }

    public List<FolderDto> getRecent() {
        return recent;
    }

    public void setRecent(List<FolderDto> recent) {
        this.recent = recent;
    }
}
