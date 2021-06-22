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

package net.opentsdb.horizon.config;

import java.util.Map;

public class DBConfig {
    public String dbUsername;
    public String dbKey;
    public String dbRWUrl;
    public String dbROUrl;
    public String dbName;
    public Map<String, String> jdbcProperties;

    public int c3p0MinPoolSize;
    public int c3p0MaxPoolSize;
    public int c3p0LoginTimeout;
    public int c3p0MaxStatements;
    public int c3p0MaxStatementsPerConnection;
    public int c3p0MaxConnectionAge;
    public int c3p0MaxIdleTime;
    public int c3p0MaxIdleTimeExcessConnections;
    public int c3p0IdleConnectionTestPeriod;
    public boolean c3p0TestConnectionOnCheckin;
    public boolean c3p0TestConnectionOnCheckout;
}
