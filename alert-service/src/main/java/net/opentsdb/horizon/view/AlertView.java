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
import net.opentsdb.horizon.model.AlertType;

import java.util.List;
import java.util.Map;

import static net.opentsdb.horizon.converter.BaseConverter.NOT_PASSED;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AlertView extends BaseDto {

  private long id = NOT_PASSED;
  private String name;
  private String slug;
  private String namespace;
  private AlertType type;
  private Boolean enabled;
  private boolean deleted;
  private int version = NOT_PASSED;
  private List<String> labels;
  private Map<String, Object> queries;
  private Map<String, Object> threshold;
  private Map<String, Object> notification;
  private List<String> alertGroupingRules;
  private BatchContact recipients; // for summary view
  private Map<String, Object> createdFrom;

  @JsonIgnore private int namespaceid;

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

  public String getSlug() {
    return slug;
  }

  public void setSlug(String slug) {
    this.slug = slug;
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public AlertType getType() {
    return type;
  }

  public void setType(AlertType type) {
    this.type = type;
  }

  public Boolean getEnabled() {
    return enabled;
  }

  public void setEnabled(Boolean enabled) {
    this.enabled = enabled;
  }

  public boolean getDeleted() {
    return deleted;
  }

  public void setDeleted(final boolean deleted) {
    this.deleted = deleted;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public List<String> getLabels() {
    return labels;
  }

  public void setLabels(List<String> labels) {
    this.labels = labels;
  }

  public Map<String, Object> getQueries() {
    return queries;
  }

  public void setQueries(Map<String, Object> queries) {
    this.queries = queries;
  }

  public Map<String, Object> getThreshold() {
    return threshold;
  }

  public void setThreshold(Map<String, Object> threshold) {
    this.threshold = threshold;
  }

  public Map<String, Object> getNotification() {
    return notification;
  }

  public void setNotification(Map<String, Object> notification) {
    this.notification = notification;
  }

  public List<String> getAlertGroupingRules() {
    return alertGroupingRules;
  }

  public void setAlertGroupingRules(List<String> alertGroupingRules) {
    this.alertGroupingRules = alertGroupingRules;
  }

  public BatchContact getRecipients() {
    return recipients;
  }

  public void setRecipients(BatchContact recipients) {
    this.recipients = recipients;
  }

  public Map<String, Object> getCreatedFrom() {
    return createdFrom;
  }

  public void setCreatedFrom(Map<String, Object> createdFrom) {
    this.createdFrom = createdFrom;
  }

  public int getNamespaceId() {
    return namespaceid;
  }

  public void setNamespaceId(int namespaceid) {
    this.namespaceid = namespaceid;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof AlertView)) {
      return false;
    }
    AlertView that = (AlertView) obj;

    if (this.id != NOT_PASSED && that.id != NOT_PASSED) {
      return this.id == that.id;
    }
    return this.name.equals(that.name) && this.namespace.equalsIgnoreCase(that.namespace);
  }

  @Override
  public int hashCode() {
    return (this.name.hashCode() + this.namespace.hashCode()) * 31;
  }
}
