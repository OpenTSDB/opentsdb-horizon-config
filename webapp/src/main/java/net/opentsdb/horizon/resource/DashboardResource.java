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
import net.opentsdb.horizon.fs.view.FileDto;
import net.opentsdb.horizon.fs.view.FolderDto;
import net.opentsdb.horizon.service.DashboardService;
import net.opentsdb.horizon.view.MoveRequest;
import net.opentsdb.horizon.view.UserFolderDto;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Example;
import io.swagger.annotations.ExampleProperty;
import net.opentsdb.servlet.resources.ServletResource;

import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.Size;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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

import static net.opentsdb.horizon.util.Utils.isNullOrEmpty;

@Api("Dashboards")
@Path("/v1/dashboard")
public class DashboardResource extends BaseTSDBPlugin implements ServletResource {
    private static final String TYPE = "DashboardResource";

    private DashboardService dashboardService;

    public DashboardResource() {

    }

    public DashboardResource(DashboardService dashboardService) {
        this.dashboardService = dashboardService;
    }

    @Override
    public Deferred<Object> initialize(TSDB tsdb, String id) {
      this.tsdb = tsdb;
      this.id = id;

      Object temp = tsdb.getRegistry().getSharedObject(DashboardService.SO_SERVICE);
      if (temp == null) {
        return Deferred.fromError(new RuntimeException("No " + DashboardService.SO_SERVICE
                + " in the shared objects registry."));
      }
      dashboardService = (DashboardService) temp;

      return Deferred.fromResult(null);
    }

    @Override
    public String type() {
      return TYPE;
    }

    @ApiOperation(value = "Create or Update Folder", notes = "If 'id' is not passed in body, It adds a new folder. Otherwise it updates the name of an existing folder "
        + "and updates the path of all the subfolders and files recursively")
    @PUT
    @Path("folder")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createOrUpdateFolder(FolderDto folder, @Context HttpServletRequest request) {
        try {
            return dashboardService.createOrUpdateFolder(folder, request);
        } catch (BadRequestException ex) {
            return Response.status(Response.Status.BAD_REQUEST).entity(ex.getMessage()).build();
        }
    }

    @ApiOperation(value = "Create or Update File", notes = "If 'id' is not passed in body, It adds a new file. Otherwise it updates an existing file")
    @PUT
    @Path("file")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createOrUpdateFile(FileDto file, @Context HttpServletRequest request) {
        try {
            return dashboardService.createOrUpdateFile(file, request);
        } catch (BadRequestException ex) {
            return Response.status(Response.Status.BAD_REQUEST).entity(ex.getMessage()).build();
        }
    }

    @ApiOperation(value = "Move Folder", notes = "Updates the path of all the subfolders and files recursively")
    @PUT
    @Path("folder/move")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response moveFolder(MoveRequest moveRequest, @Context HttpServletRequest request) {
        FolderDto folder = dashboardService.moveFolder(moveRequest, request);
        return Response.status(Response.Status.OK).entity(folder).build();
    }

    @ApiOperation("Get Folder by Id")
    @GET
    @Path("folder/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getFolderById(@PathParam("id") long id, @Context HttpServletRequest request) {
        FolderDto folder = dashboardService.getFolderById(id, request.getUserPrincipal().getName());
        final Response.ResponseBuilder responseBuilder = folder == null ? Response.status(Response.Status.NOT_FOUND) : Response.status(Response.Status.OK);
        return responseBuilder.entity(folder).build();
    }

    @ApiOperation("Get File by Id")
    @GET
    @Path("file/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getFileById(@PathParam("id") long id, @Context HttpServletRequest request) {
        FileDto file = dashboardService.getFileById(id, request.getUserPrincipal().getName());
        final Response.ResponseBuilder responseBuilder = file == null ? Response.status(Response.Status.NOT_FOUND) : Response.status(Response.Status.OK);
        return responseBuilder.entity(file).build();
    }

    @ApiOperation("Get by path")
    @GET
    @Path("{path:.*}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getByPath(@PathParam("path") String path, @Context HttpServletRequest request) {
        FolderDto folder = dashboardService.getByPath(path, request.getUserPrincipal().getName());
        final Response.ResponseBuilder responseBuilder = folder == null ? Response.status(Response.Status.NOT_FOUND) : Response.status(Response.Status.OK);
        return responseBuilder.entity(folder).build();
    }

    @ApiOperation(value = "Top Folders", notes = "List top level folders for a user or namespace. By default lists the top level folders for logged in User. Set the appropriate query param to list for different user or namespace.")
    @GET
    @Path("/topFolders")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response topFolders(
        @ApiParam(value = "List top level folders for this user", format = "user.userId", examples = @Example(value = @ExampleProperty(value = "user.bob"))) @Size(min = 1, max = 128) @QueryParam("userId") String userId,
        @ApiParam(value = "List top level folders for this namespace", format = "namespace name", examples = @Example(value = @ExampleProperty(value = "myns"))) @QueryParam("namespace") String namespace,
        @Context HttpServletRequest request) {

        Object folder;
        if (!isNullOrEmpty(userId)) {
            folder = dashboardService.getUserFolder(userId);
        } else if (!isNullOrEmpty(namespace)) {
            folder = dashboardService.getNamespaceFolder(namespace);
        } else {
            folder = dashboardService.getUserFolder(request.getUserPrincipal().getName());
        }

        final Response.ResponseBuilder responseBuilder =
            folder == null ? Response.status(Response.Status.NOT_FOUND)
                : Response.status(Response.Status.OK);
        return responseBuilder.entity(folder).build();
    }

  @ApiOperation(
      value = "Get recent dashboards",
      notes =
          "List the most recently visited folders and dashboards of the logged in user unless a different user id is passed.")
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
    UserFolderDto userFolder = dashboardService.getRecentlyVisited(userId, limit);
    return Response.status(Response.Status.OK).entity(userFolder).build();
  }

  @ApiOperation(
      value = "Get Favorite Dashboards",
      notes =
          "List the favorite folders and dashboards of the logged in user unless a different user id is passed.")
  @GET
  @Path("/favorite")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getFavorites(
      @ApiParam(name = "userId", value = "user id", format = "user.userId") @QueryParam("userId") String userId,
      @Context HttpServletRequest request) {
    if (isNullOrEmpty(userId)) {
      userId = request.getUserPrincipal().getName();
    }
    UserFolderDto userFolder = dashboardService.getFavorites(userId);
    return Response.status(Response.Status.OK).entity(userFolder).build();
  }

  @ApiOperation(value = "Add to my Favorites", notes = "Add dashboard or folder to user's favorites")
  @POST
  @Path("/favorite")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response addToFavorites(final FolderDto folder, @Context HttpServletRequest request) {
    String userId = request.getUserPrincipal().getName();
    dashboardService.addToFavorites(folder.getId(), userId);
    return Response.status(Response.Status.CREATED).build();
  }

  @ApiOperation(
      value = "Remove from my Favorites",
      notes = "Remove dashboard or folder from user's favorites")
  @DELETE
  @Path("/favorite")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteFromFavorites(
      final FolderDto folder, @Context final HttpServletRequest request) {
    String userId = request.getUserPrincipal().getName();
    dashboardService.deleteFromFavorites(folder.getId(), userId);
    return Response.status(Response.Status.NO_CONTENT).build();
  }
}
