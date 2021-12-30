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
import net.opentsdb.horizon.service.SnapshotService;
import net.opentsdb.horizon.view.SnapshotView;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import net.opentsdb.servlet.resources.ServletResource;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
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
import java.util.List;

import static net.opentsdb.horizon.util.Utils.isNullOrEmpty;

@Api("Snapshots")
@Path("v1/snapshot")
public class SnapshotResource extends BaseTSDBPlugin implements ServletResource {
  private static final String TYPE = "SnapshotResource";

  private SnapshotService service;

  public SnapshotResource() {

  }

  public SnapshotResource(SnapshotService service) {
    this.service = service;
  }

  @Override
  public Deferred<Object> initialize(TSDB tsdb, String id) {
    this.tsdb = tsdb;
    this.id = id;

    Object temp = tsdb.getRegistry().getSharedObject(SnapshotService.SO_SERVICE);
    if (temp == null) {
      return Deferred.fromError(new RuntimeException("No " + SnapshotService.SO_SERVICE
              + " in the shared objects registry."));
    }
    service = (SnapshotService) temp;

    return Deferred.fromResult(null);
  }

  @Override
  public String type() {
    return TYPE;
  }

  @ApiOperation("Create Snapshot")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response create(SnapshotView view, @Context HttpServletRequest request) {
    SnapshotView snapshot = service.create(view, request.getUserPrincipal().getName());
    return Response.status(Response.Status.CREATED).entity(snapshot).build();
  }

  @ApiOperation(value = "Update Snapshot")
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response update(SnapshotView view, @Context HttpServletRequest request) {
    SnapshotView snapshot = service.update(view, request.getUserPrincipal().getName());
    return Response.status(Response.Status.OK).entity(snapshot).build();
  }

  @ApiOperation("Get by id")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("{id}")
  public Response getById(@PathParam("id") long id, @Context HttpServletRequest request) {
    SnapshotView view = service.getById(id, request.getUserPrincipal().getName());
    return Response.status(Response.Status.OK).entity(view).build();
  }

  @ApiOperation(
      value = "Get recently visited snapshots",
      notes =
          "List the most recently visited snapshots of the logged in user unless a different user id is passed.")
  @GET
  @Path("/recent")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRecentlyVisited(
      @ApiParam(name = "userId", value = "user id", format = "user.userId") @QueryParam("userId")
          String userId,
      @ApiParam(name = "limit", value = "limit") @DefaultValue("50") @QueryParam("limit") int limit,
      @Context HttpServletRequest request) {
    if (isNullOrEmpty(userId)) {
      userId = request.getUserPrincipal().getName();
    }
    List<SnapshotView> snapshots = service.getRecentlyVisited(userId, limit);
    return Response.status(Response.Status.OK).entity(snapshots).build();
  }
}
