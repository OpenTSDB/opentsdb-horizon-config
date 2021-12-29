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

package net.opentsdb.horizon.resource;

import com.stumbleupon.async.Deferred;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.horizon.NamespaceCache;
import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.service.SnoozeService;
import net.opentsdb.horizon.view.SnoozeView;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import net.opentsdb.servlet.resources.ServletResource;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static net.opentsdb.horizon.converter.BatchContactConverter.NOT_PASSED;
import static net.opentsdb.horizon.profile.Utils.validateNamespace;

@Api("Snooze at namespace level")
@Path("v1/namespace/{namespace}/snooze")
public class NamespaceSnoozeResource extends BaseTSDBPlugin implements ServletResource {
  private static final String TYPE = "NamespaceSnoozeResource";

  private SnoozeService service;
  private NamespaceCache namespaceCache;

  public NamespaceSnoozeResource() {

  }

  public NamespaceSnoozeResource(final SnoozeService service,
                                 final NamespaceCache namespaceCache) {
    this.service = service;
    this.namespaceCache = namespaceCache;
  }

  @Override
  public Deferred<Object> initialize(TSDB tsdb, String id) {
    this.tsdb = tsdb;
    this.id = id;

    Object temp = tsdb.getRegistry().getSharedObject(SnoozeService.SO_SERVICE);
    if (temp == null) {
      return Deferred.fromError(new RuntimeException("No " + SnoozeService.SO_SERVICE
              + " in the shared objects registry."));
    }
    service = (SnoozeService) temp;

    temp = tsdb.getRegistry().getSharedObject(NamespaceCache.SO_NAMESPACE_CACHE);
    if (temp == null) {
      return Deferred.fromError(new RuntimeException("No " + NamespaceCache.SO_NAMESPACE_CACHE
              + " in the shared objects registry."));
    }
    namespaceCache = (NamespaceCache) temp;

    return Deferred.fromResult(null);
  }

  @Override
  public String type() {
    return TYPE;
  }

  @ApiOperation("Create in a namespace")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response create(
      @PathParam("namespace") String namespace,
      List<SnoozeView> snoozes,
      @Context HttpServletRequest request) {
    Namespace ns;
    try {
      ns = namespaceCache.getByName(namespace);
    } catch (Exception e) {
      String message = "Error reading namespace with name: " + namespace;
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(message).build();
    }
    validateNamespace(ns, namespace);
    snoozes.stream().forEach(snoozeView -> snoozeView.setNamespace(namespace));

    final List<SnoozeView> responseList =
        service.creates(snoozes, ns, request.getUserPrincipal().getName());

    return Response.status(Response.Status.CREATED).entity(responseList).build();
  }

  @ApiOperation("Update")
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response update(
      @PathParam("namespace") String namespace,
      List<SnoozeView> updates,
      @Context HttpServletRequest request) {
    Namespace ns;
    try {
      ns = namespaceCache.getByName(namespace);
    } catch (Exception e) {
      String message = "Error reading namespace with name: " + namespace;
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(message).build();
    }
    validateNamespace(ns, namespace);

    final List<SnoozeView> creates =
        updates.stream()
            .map(
                snoozeView -> {
                  snoozeView.setNamespace(namespace);
                  return snoozeView;
                })
            .filter(snoozeView -> snoozeView.getId() == NOT_PASSED)
            .collect(Collectors.toList());

    updates.removeAll(creates);

    final String principal = request.getUserPrincipal().getName();
    final List<SnoozeView> responseList = new ArrayList<>();
    if (!creates.isEmpty()) {
      responseList.addAll(service.creates(creates, ns, principal));
    }

    if (!updates.isEmpty()) {
      responseList.addAll(service.updates(updates, ns, principal));
    }

    return Response.status(Response.Status.OK).entity(responseList).build();
  }

  @ApiOperation("Get all for a namespace")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSnoozesForNamespace(@PathParam("namespace") String namespace) {
    Namespace ns;
    try {
      ns = namespaceCache.getByName(namespace);
    } catch (Exception e) {
      String message = "Error reading namespace with name: " + namespace;
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(message).build();
    }
    validateNamespace(ns, namespace);

    final List<SnoozeView> forNamespace = service.getForNamespace(namespace);
    return Response.status(Response.Status.OK).entity(forNamespace).build();
  }
}
