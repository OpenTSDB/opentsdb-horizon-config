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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonIgnoreProperties(ignoreUnknown = true)
public class BatchContact {

  List<EmailContact> email;

  List<SlackContact> slack;

  List<OpsGenieContact> opsgenie;

  List<HttpContact> http;

  List<OCContact> oc;

  @JsonIgnore
  private int namespaceid;

  public List<EmailContact> getEmail() {
    return email;
  }

  public void setEmail(List<EmailContact> email) {
    this.email = email;
  }

  public List<SlackContact> getSlack() {
    return slack;
  }

  public void setSlack(List<SlackContact> slack) {
    this.slack = slack;
  }

  public List<OpsGenieContact> getOpsgenie() {
    return opsgenie;
  }

  public void setOpsgenie(List<OpsGenieContact> opsgenie) {
    this.opsgenie = opsgenie;
  }

  public List<HttpContact> getHttp() {
    return http;
  }

  public void setHttp(List<HttpContact> http) {
    this.http = http;
  }

  public List<OCContact> getOc() {
    return oc;
  }

  public void setOc(List<OCContact> oc) {
    this.oc = oc;
  }

  public int getNamespaceId() {
    return namespaceid;
  }

  public void setNamespaceId(int namespaceid) {
    this.namespaceid = namespaceid;
  }
}
