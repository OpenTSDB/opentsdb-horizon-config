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

import net.opentsdb.horizon.view.BatchContact;
import net.opentsdb.horizon.view.BaseDto;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Transient;
import java.sql.Timestamp;
import java.util.Map;

@Getter
@Setter
@Builder
public class Snooze extends BaseDto {

    private long id;
    private Timestamp startTime;
    private Timestamp endTime;
    private int namespaceId;
    @Builder.Default private boolean enabled = true;
    @Builder.Default private boolean deleted = false;
    private Map<String, Object> definition;

    @Transient private BatchContact batchContact;
    @Transient private Object contact;

}
