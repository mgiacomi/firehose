package com.gltech.scale.core.writer;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

@Path("/collector")
public class StorageWriteResource
{
	@Context
	UriInfo uriInfo;

	@Path("/test")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response test()
	{
		return Response.ok("{}", MediaType.APPLICATION_JSON_TYPE).build();
	}
}
