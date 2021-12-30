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
import net.opentsdb.horizon.model.ContactType;
import net.opentsdb.horizon.model.Namespace;
import net.opentsdb.horizon.service.ContactService;
import net.opentsdb.horizon.view.BatchContact;
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
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static net.opentsdb.horizon.profile.Utils.validateNamespace;

@Api("Contacts")
@Path("/v1/namespace/{namespace}/contact")
public class ContactsResource extends BaseTSDBPlugin implements ServletResource {
  private static final String TYPE = "ContactsResource";

  private ContactService service;
  private NamespaceCache namespaceCache;

  public ContactsResource() {

  }

  public ContactsResource(final ContactService service, final NamespaceCache namespaceCache) {
    this.service = service;
    this.namespaceCache = namespaceCache;
  }

  @Override
  public Deferred<Object> initialize(TSDB tsdb, String id) {
    this.tsdb = tsdb;
    this.id = id;

    Object temp = tsdb.getRegistry().getSharedObject(ContactService.SO_SERVICE);
    if (temp == null) {
      return Deferred.fromError(new RuntimeException("No " + ContactService.SO_SERVICE
              + " in the shared objects registry."));
    }
    service = (ContactService) temp;

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

  @ApiOperation("Create")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response create(
      @PathParam("namespace") String namespace,
      BatchContact contacts,
      @Context HttpServletRequest request) {
    Namespace n;
    try {
      n = namespaceCache.getByName(namespace);
    } catch (Exception e) {
      String message = "Error reading namespace with name: " + namespace;
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(message).build();
    }
    validateNamespace(n, namespace);
    contacts.setNamespaceId(n.getId());
    BatchContact view = service.create(contacts, n, request.getUserPrincipal().getName());
    return Response.status(Response.Status.CREATED).entity(view).build();
  }

  @ApiOperation("Get by namespace and type")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getContact(
      @PathParam("namespace") String namespace, @QueryParam("type") String type) {
    BatchContact contacts;
    if (type == null) {
      contacts = service.getContactsByNamespace(namespace);
    } else {
      ContactType contactType = null;
      try {
        contactType = ContactType.valueOf(type.toLowerCase());
      } catch (IllegalArgumentException ex) {
        Response.status(Response.Status.BAD_REQUEST).entity(ex.getMessage()).build();
      }

      contacts = service.getContactsByNamespaceAndType(namespace, contactType);
    }
    return Response.status(Response.Status.OK).entity(contacts).build();
  }

  @ApiOperation("Get contacts for alert")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("{alertid}")
  public Response getContact(@PathParam("alertid") long alertId) {
    BatchContact contacts = service.getContactForAlert(alertId);
    return Response.status(Response.Status.OK).entity(contacts).build();
  }

  @ApiOperation("Update")
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response update(
      @PathParam("namespace") String namespace,
      BatchContact contacts,
      @Context HttpServletRequest request) {
    Namespace n;
    try {
      n = namespaceCache.getByName(namespace);
    } catch (Exception e) {
      String message = "Error reading namespace with name: " + namespace;
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(message).build();
    }
    validateNamespace(n, namespace);
    service.update(contacts, n, request.getUserPrincipal().getName());
    return Response.status(Response.Status.OK).entity(contacts).build();
  }

  @ApiOperation("Delete")
  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/delete")
  public Response deleteContacts(
      @PathParam("namespace") String namespace,
      BatchContact contacts,
      @Context HttpServletRequest request) {
    Namespace n;
    try {
      n = namespaceCache.getByName(namespace);
    } catch (Exception e) {
      String message = "Error reading namespace with name: " + namespace;
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(message).build();
    }
    validateNamespace(n, namespace);
    contacts.setNamespaceId(n.getId());
    service.delete(contacts, n, request.getUserPrincipal().getName());
    return Response.status(Response.Status.OK).entity(contacts).build();
  }
}
