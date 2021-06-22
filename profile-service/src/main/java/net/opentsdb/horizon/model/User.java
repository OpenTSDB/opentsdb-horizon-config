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

package net.opentsdb.horizon.model;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.annotations.ApiParam;
import java.sql.Timestamp;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class User {

    private String userid;
    private String name;
    private Boolean enabled;

    @ApiParam(value = "created on the fly or by offline job", required = true, defaultValue = "onthefly", allowableValues = "onthefly, offline")
    private CreationMode creationmode;

    @ApiParam(hidden = true)
    private Timestamp updatedtime;

    private Timestamp disabledtime;

    public String getUserid() {
        return userid;
    }

    public User setUserid(String userid) {
        this.userid = userid;
        return this;
    }

    public String getName() {
        return name;
    }

    public User setName(String name) {
        this.name = name;
        return this;
    }

    public Boolean isEnabled() {
        return enabled;
    }

    public User setEnabled(Boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public CreationMode getCreationmode() {
        return creationmode;
    }

    public User setCreationmode(CreationMode creationmode) {
        this.creationmode = creationmode;
        return this;
    }

    public Timestamp getUpdatedtime() {
        return updatedtime;
    }

    public void setUpdatedtime(Timestamp updatedtime) {
        this.updatedtime = updatedtime;
    }

    public Timestamp getDisabledtime() {
        return disabledtime;
    }

    public void setDisabledtime(Timestamp disabledtime) {
        this.disabledtime = disabledtime;
    }

    @Override
    public int hashCode() {
        return userid.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof User)) {
            return false;
        }
        User that = (User) obj;
        return this.userid.equals(that.userid);
    }

    public enum CreationMode {
            onthefly((byte) 0), offline((byte) 1), migration((byte) 2), notcreated((byte)3);

        public final byte id;

        CreationMode(byte id) {
            this.id = id;
        }
    }
}
