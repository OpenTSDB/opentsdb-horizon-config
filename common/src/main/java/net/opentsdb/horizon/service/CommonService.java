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

package net.opentsdb.horizon.service;

import net.opentsdb.horizon.converter.BaseConverter;
import net.opentsdb.horizon.model.CommonModel;
import net.opentsdb.horizon.store.BaseStore;

import java.sql.Timestamp;

public abstract class CommonService<View, Model extends CommonModel, Converter extends BaseConverter<View, Model>>
    extends BaseService<View, Model, Converter> {

  public CommonService(Converter converter, BaseStore store) {
    super(converter, store);
  }

  @Override
  protected void setCreatorIdAndTime(Model model, String principal, Timestamp timestamp) {
    model.setCreatedBy(principal);
    model.setCreatedTime(timestamp);
  }

  @Override
  protected void setUpdaterIdAndTime(Model model, String principal, Timestamp timestamp) {
    model.setUpdatedBy(principal);
    model.setUpdatedTime(timestamp);
  }
}
