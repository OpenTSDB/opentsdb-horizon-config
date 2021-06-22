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

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class OCContact extends BaseContact {

  private String displaycount;

  private String customer;

  private String context;

  private String opsdbproperty;

  public String getDisplaycount() {
    return displaycount;
  }

  public void setDisplaycount(String displaycount) {
    this.displaycount = displaycount;
  }

  public String getCustomer()
  {
    return customer;
  }

  public void setCustomer(String customer)
  {
    this.customer = customer;
  }

  public String getContext() {
    return context;
  }

  public void setContext(String context) {
    this.context = context;
  }

  public String getOpsdbproperty() {
    return opsdbproperty;
  }

  public void setOpsdbproperty(String opsdbproperty) {
    this.opsdbproperty = opsdbproperty;
  }
}
