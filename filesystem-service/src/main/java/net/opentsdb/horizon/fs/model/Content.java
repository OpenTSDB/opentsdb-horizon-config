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

import java.sql.Timestamp;

public class Content {
    private byte[] sha2;

    private byte[] data;

    private String createdby;

    private Timestamp createdtime;

    public Content() {
    }

    public Content(byte[] sha2, byte[] data) {
        this.sha2 = sha2;
        this.data = data;
    }

    public byte[] getSha2() {
        return sha2;
    }

    public void setSha2(byte[] sha2) {
        this.sha2 = sha2;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public String getCreatedby() {
        return createdby;
    }

    public void setCreatedby(String createdby) {
        this.createdby = createdby;
    }

    public Timestamp getCreatedtime() {
        return createdtime;
    }

    public void setCreatedtime(Timestamp createdtime) {
        this.createdtime = createdtime;
    }
}
