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
import net.opentsdb.horizon.service.AlertService;
import net.opentsdb.horizon.view.AlertView;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import net.opentsdb.servlet.resources.ServletResource;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

import static net.opentsdb.horizon.converter.BatchContactConverter.NOT_PASSED;
import static net.opentsdb.horizon.profile.Utils.validateNamespace;

@Api("Namespace Alerts")
@Path("v1/namespace/{namespace}/alert")
public class NamespaceAlertResource extends BaseTSDBPlugin implements ServletResource {
  private static final String TYPE = "NamespaceAlertResource";

  private AlertService service;
  private NamespaceCache namespaceCache;

  public NamespaceAlertResource() {

  }

  public NamespaceAlertResource(final AlertService service,
                                final NamespaceCache namespaceCache) {
    this.service = service;
    this.namespaceCache = namespaceCache;
  }

  @Override
  public Deferred<Object> initialize(TSDB tsdb, String id) {
    this.tsdb = tsdb;
    this.id = id;

    Object temp = tsdb.getRegistry().getSharedObject(AlertService.SO_SERVICE);
    if (temp == null) {
      return Deferred.fromError(new RuntimeException("No " + AlertService.SO_SERVICE
              + " in the shared objects registry."));
    }
    service = (AlertService) temp;

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

  @ApiOperation("Create for a namespace")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response create(
      @PathParam("namespace") String namespace,
      List<AlertView> alerts,
      @Context HttpServletRequest request) {
    Namespace ns;
    try {
      ns = namespaceCache.getByName(namespace);
    } catch (Exception e) {
      String message = "Error reading namespace with name: " + namespace;
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(message).build();
    }
    validateNamespace(ns, namespace);
    alerts.stream()
        .forEach(
            alert -> {
              alert.setNamespace(namespace);
              alert.setNamespaceId(ns.getId());
            });

    List<AlertView> view = service.creates(alerts, ns, request.getUserPrincipal().getName());
    return Response.status(Response.Status.CREATED).entity(view).build();
  }

  @ApiOperation("Get by namespace and name")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("{name}")
  public Response getAlertByName(
      @PathParam("namespace") String namespace,
      @PathParam("name") String name,
      @ApiParam(defaultValue = "false") @QueryParam("definition") boolean fetchDefinition,
      @ApiParam(defaultValue = "false") @QueryParam("deleted") boolean deleted) {

    AlertView alert = service.getByNamespaceAndName(namespace, name, fetchDefinition, deleted);
    return Response.status(Response.Status.OK).entity(alert).build();
  }

  @ApiOperation("Get by namespace")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAlert(
      @PathParam("namespace") String namespace,
      @ApiParam(defaultValue = "false") @QueryParam("definition") boolean fetchDefinition,
      @ApiParam(defaultValue = "false") @QueryParam("deleted") boolean deleted) {
    List<AlertView> alerts = service.getByNamespace(namespace, fetchDefinition, deleted);
    return Response.status(Response.Status.OK).entity(alerts).build();
  }

  @ApiOperation("Update")
  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  public Response updateAlert(
      @PathParam("namespace") String namespace,
      List<AlertView> alerts,
      @Context HttpServletRequest request) {
    Namespace ns;
    try {
      ns = namespaceCache.getByName(namespace);
    } catch (Exception e) {
      String message = "Error reading namespace with name: " + namespace;
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(message).build();
    }
    validateNamespace(ns, namespace);
    List<AlertView> newAlerts = new ArrayList<>();
    alerts.stream()
        .forEach(
            alert -> {
              if (alert.getId() == NOT_PASSED) {
                newAlerts.add(alert);
              }
              alert.setNamespace(namespace);
              alert.setNamespaceId(ns.getId());
            });
    alerts.removeAll(newAlerts);

    String principal = request.getUserPrincipal().getName();
    List<AlertView> responseList = new ArrayList<>();

    if (!newAlerts.isEmpty()) {
      List<AlertView> created = service.creates(newAlerts, ns, principal);
      responseList.addAll(created);
    }
    if (!alerts.isEmpty()) {
      List<AlertView> updated = service.updates(alerts, ns, principal);
      responseList.addAll(updated);
    }

    return Response.status(Response.Status.OK).entity(responseList).build();
  }

  @ApiOperation("Delete")
  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("delete")
  public Response deleteAlert(
      @PathParam("namespace") String namespaceName,
      long[] ids,
      @Context HttpServletRequest request) {
    Namespace namespace = null;
    try {
      namespace = namespaceCache.getByName(namespaceName);
    } catch (Exception e) {
      String message = "Error reading namespace with name: " + namespace;
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(message).build();
    }
    validateNamespace(namespace, namespaceName);
    service.deleteById(namespace, ids, request.getUserPrincipal().getName());
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  @ApiOperation("Restore")
  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("restore")
  public Response restoreAlert(
      @PathParam("namespace") String namespaceName,
      long[] ids,
      @Context HttpServletRequest request) {
    Namespace namespace = null;
    try {
      namespace = namespaceCache.getByName(namespaceName);
    } catch (Exception e) {
      String message = "Error reading namespace with name: " + namespace;
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(message).build();
    }
    validateNamespace(namespace, namespaceName);
    service.restore(namespace, ids, request.getUserPrincipal().getName());
    return Response.status(Response.Status.NO_CONTENT).build();
  }
}
