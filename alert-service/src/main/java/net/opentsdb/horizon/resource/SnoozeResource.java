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

import net.opentsdb.horizon.service.SnoozeService;
import net.opentsdb.horizon.view.SnoozeView;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Api("Snooze")
@Path("v1/snooze")
public class SnoozeResource {

  private SnoozeService service;

  public SnoozeResource(final SnoozeService service) {
    this.service = service;
  }

  @ApiOperation("Get by id")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("{id}")
  public Response getSnoozeById(@PathParam("id") long id) {
    final SnoozeView byId = service.getById(id);
    return Response.status(Response.Status.OK).entity(byId).build();
  }

  @ApiOperation("Hard Delete")
  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("delete")
  public Response deleteSnooze(long[] ids, @Context HttpServletRequest request) {
    service.deleteByIds(ids, request.getUserPrincipal().getName());
    return Response.status(Response.Status.OK).entity(ids).build();
  }
}
