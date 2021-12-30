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
import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.service.NamespaceFollowerService;
import net.opentsdb.horizon.service.NamespaceMemberService;
import net.opentsdb.horizon.service.NamespaceService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import net.opentsdb.servlet.resources.ServletResource;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
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

@Api("Namespace")
@Path("/v1/namespace")
public class NamespaceResource extends BaseTSDBPlugin implements ServletResource {
    private static final String TYPE = "NamespaceResource";

    private NamespaceService service;
    private NamespaceMemberService namespaceMemberService;
    private NamespaceFollowerService followerService;

    public NamespaceResource() {

    }

    public NamespaceResource(final NamespaceService service,
                             final NamespaceMemberService namespaceMemberService,
                             final NamespaceFollowerService followerService) {
        this.service = service;
        this.namespaceMemberService = namespaceMemberService;
        this.followerService = followerService;
    }

    public Deferred<Object> initialize(TSDB tsdb, String id) {
        this.tsdb = tsdb;
        this.id = id;

        Object temp = tsdb.getRegistry().getSharedObject(NamespaceService.SO_SERVICE);
        if (temp == null) {
            return Deferred.fromError(new RuntimeException("No " + NamespaceService.SO_SERVICE
                    + " in the shared objects registry."));
        }
        service = (NamespaceService) temp;

        temp = tsdb.getRegistry().getSharedObject(NamespaceMemberService.SO_SERVICE);
        if (temp == null) {
            return Deferred.fromError(new RuntimeException("No " + NamespaceMemberService.SO_SERVICE
                    + " in the shared objects registry."));
        }
        namespaceMemberService = (NamespaceMemberService) temp;

        temp = tsdb.getRegistry().getSharedObject(NamespaceFollowerService.SO_SERVICE);
        if (temp == null) {
            return Deferred.fromError(new RuntimeException("No " + NamespaceFollowerService.SO_SERVICE
                    + " in the shared objects registry."));
        }
        followerService = (NamespaceFollowerService) temp;

        return Deferred.fromResult((Object)null);
    }

    @Override
    public String type() {
        return TYPE;
    }

    @ApiOperation("Get All Namespaces")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllNamespace(@QueryParam("name") String name, @QueryParam("alias") String alias, @QueryParam("includeNameOrAlias") boolean includeNameOrAlias, @Context HttpServletRequest request) {
        if (name == null && alias == null) { // get all namespace
            final List<Namespace> namespaceList = service.getAll();
            return Response.status(Response.Status.OK).entity(namespaceList).build();
        } else {
            // search by namespace name or alias; ideally should found one single namespace
            final Namespace namespace = service.getNamespace(name, alias, includeNameOrAlias);
            final Response.ResponseBuilder responseBuilder = (namespace == null) ? Response.status(Response.Status.NOT_FOUND) : Response.status(Response.Status.OK);
            return responseBuilder.entity(namespace).build();
        }
    }


    @ApiOperation("Get by id")
    @GET
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getById(@PathParam("id") int namespaceId, @Context HttpServletRequest request) {
        final Namespace namespace = service.getNamespace(namespaceId);
        final Response.ResponseBuilder responseBuilder = namespace == null ? Response.status(Response.Status.NOT_FOUND) : Response.status(Response.Status.OK);
        return responseBuilder.entity(namespace).build();
    }

    @ApiOperation("Create Namespace")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response create(Namespace namespace, @Context HttpServletRequest request){
        try {
            namespace = service.create(namespace, request.getUserPrincipal().getName());
            return Response.status(Response.Status.CREATED).entity(namespace).build();
        }catch (BadRequestException ex){
            return Response.status(Response.Status.BAD_REQUEST).entity(ex.getMessage()).build();
        }
    }

    @ApiOperation("Update by name or alias")
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response editNamespace(@QueryParam("name") String name, @QueryParam("alias") String alias, Namespace namespace, @Context HttpServletRequest request) {
        try {
            namespace = service.update(name, alias, namespace, request);
            return Response.status(Response.Status.CREATED).entity(namespace).build();
        } catch (NotFoundException ex) {
            return Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage()).build();
        } catch (BadRequestException ex) {
            return Response.status(Response.Status.BAD_REQUEST).entity(ex.getMessage()).build();
        } catch (ForbiddenException ex) {
            return Response.status(Response.Status.FORBIDDEN).entity(ex.getMessage()).build();
        }
    }


    @ApiOperation("Update by id")
    @PUT
    @Path("/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response editNamespace(@PathParam("id") int namespaceId, Namespace namespace, @Context HttpServletRequest request){
        namespace = service.update(namespaceId, namespace, request);
        return Response.status(Response.Status.OK).entity(namespace).build();
    }

    /***********************************************************************************************************************************************************
                                                        NAMESPACE MEMBER RESOURCE
     ************************************************************************************************************************************************************/

    @ApiOperation("Get members of a namespace")
    @GET
    @Path("/{id}/member")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getNamespaceMembers(@PathParam("id") int namespaceId) {
        return Response.status(Response.Status.OK).entity(namespaceMemberService.getNamespaceMember(namespaceId)).build();
    }

    @ApiOperation("Add member to namespace")
    @POST
    @Path("/{id}/member")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response addNamespaceMember(@PathParam("id") int namespaceid, List<String> memberIdList, @Context HttpServletRequest request){
        try {
            namespaceMemberService.addNamespaceMember(namespaceid, memberIdList, request.getUserPrincipal().getName());
            return Response.status(Response.Status.CREATED).build();
        } catch (BadRequestException ex){
            return Response.status(Response.Status.BAD_REQUEST).entity(ex.getMessage()).build();
        } catch (ForbiddenException ex) {
            return Response.status(Response.Status.FORBIDDEN).entity(ex.getMessage()).build();
        }
    }

    @ApiOperation("Remove Member from namespace")
    @DELETE
    @Path("/{id}/member")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response removeNamespaceMember(@PathParam("id") int namespaceId, List<String> memberIdList, @Context HttpServletRequest request){
        try {
            namespaceMemberService.removeNamespaceMember(namespaceId, memberIdList, request.getUserPrincipal().getName());
            return Response.status(Response.Status.NO_CONTENT).build();
        } catch (BadRequestException ex){
            return Response.status(Response.Status.BAD_REQUEST).entity(ex.getMessage()).build();
        } catch (ForbiddenException ex) {
            return Response.status(Response.Status.FORBIDDEN).entity(ex.getMessage()).build();
        }
    }

    @ApiOperation(value = "Get my namespaces", notes = "Get all the namespaces, that I am member of")
    @GET
    @Path("/member")
    @Produces(MediaType.APPLICATION_JSON)
    public Response myNamespaces(@QueryParam("userid") String userId, @Context HttpServletRequest request) {
        if (isNullOrEmpty(userId)) {
            userId = request.getUserPrincipal().getName();
        }
        return Response.status(Response.Status.OK).entity(namespaceMemberService.getNamespaces(userId)).build();
    }

    /***********************************************************************************************************************************************************
                                                        NAMESPACE FOLLOWER RESOURCE
     ************************************************************************************************************************************************************/

    @ApiOperation("Get Namespace Followers")
    @GET
    @Path("/{id}/follower")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getNamespaceFollowers(@PathParam("id") int namespaceId) {
        return Response.status(Response.Status.OK).entity(followerService.getNamespaceFollowers(namespaceId)).build();
    }

    @ApiOperation("Follow a Namespace")
    @POST
    @Path("/{id}/follower")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response addNamespaceFollowers(@PathParam("id") int namespaceId, @Context HttpServletRequest request){
        try {
            followerService.addNamespaceFollower(namespaceId, request.getUserPrincipal().getName());
            return Response.status(Response.Status.CREATED).build();
        }catch (BadRequestException ex){
            return Response.status(Response.Status.BAD_REQUEST).entity(ex.getMessage()).build();
        }
    }

    @ApiOperation("Unfollow a Namespace")
    @DELETE
    @Path("/{id}/follower")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response removeNamespaceFollowers(@PathParam("id") int namespaceId, @Context HttpServletRequest request){
        try {
            followerService.removeNamespaceFollower(namespaceId, request.getUserPrincipal().getName());
            return Response.status(Response.Status.NO_CONTENT).build();
        }catch (BadRequestException ex){
            return Response.status(Response.Status.BAD_REQUEST).entity(ex.getMessage()).build();
        }
    }

    @ApiOperation("Get namespaces I follow")
    @GET
    @Path("/follower")
    @Produces(MediaType.APPLICATION_JSON)
    public Response myFollowingNamespaces(@QueryParam("userid") String userId, @Context HttpServletRequest request) {
        return Response.status(Response.Status.OK).entity(followerService.myFollowingNamespaces(userId, request)).build();
    }


}
