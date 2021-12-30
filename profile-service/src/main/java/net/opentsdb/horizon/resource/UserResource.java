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
import net.opentsdb.horizon.model.User;
import net.opentsdb.horizon.service.UserService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Example;
import io.swagger.annotations.ExampleProperty;
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
import javax.ws.rs.core.Response.Status;
import java.util.List;

@Api(value = "Users")
@Path("/v1/user")
public class UserResource extends BaseTSDBPlugin implements ServletResource {

    private static final String TYPE = "UserResource";

    private UserService userService;

    public UserResource() {

    }

    public UserResource(final UserService userService) {
        this.userService = userService;
    }

    public Deferred<Object> initialize(TSDB tsdb, String id) {
        this.tsdb = tsdb;
        this.id = id;

        Object temp = tsdb.getRegistry().getSharedObject(UserService.SO_SERVICE);
        if (temp == null) {
            return Deferred.fromError(new RuntimeException("No " + UserService.SO_SERVICE
                    + " in the shared objects registry."));
        }
        userService = (UserService) temp;

        return Deferred.fromResult(null);
    }

    @Override
    public String type() {
        return TYPE;
    }

    @ApiOperation("Create Me")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response create(@Context HttpServletRequest request) {
        return Response.status(Status.CREATED).entity(userService.create(request.getUserPrincipal().getName())).build();
    }

    @ApiOperation(value = "Provision Me", notes = "Idempotent call. Creates user, home and trash folder if not already")
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response provision(@Context HttpServletRequest request) {
        return Response.status(Status.OK).entity(userService.provision(request.getUserPrincipal().getName())).build();
    }

    @ApiOperation("Create a list of Users")
    @POST
    @Path("/list")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createMultiple(final List<User> users, @Context HttpServletRequest request) {
        return Response.status(Status.CREATED).entity(userService.creates(users, request.getUserPrincipal().getName())).build();
    }

    @ApiOperation("Get Me")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response get(@Context HttpServletRequest request) {
        String principal = request.getUserPrincipal().getName();
        return getById(principal);
    }

    @ApiOperation("Get User by Id")
    @GET
    @Path("/{userid}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getById(@ApiParam(value = "format userType.userId", examples = @Example(value = @ExampleProperty(value = "user.smrutis"))) @PathParam("userid") final String userid) {
        return Response.status(Status.OK).entity(userService.getById(userid)).build();
    }

    @ApiOperation("Get All")
    @GET
    @Path("/list")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAll(
        @ApiParam(value = "includes the disabled users", defaultValue = "false") @QueryParam("includedisabled") final boolean includeDisabled) {
        return Response.status(Status.OK).entity(userService.getAll(includeDisabled)).build();
    }

}
