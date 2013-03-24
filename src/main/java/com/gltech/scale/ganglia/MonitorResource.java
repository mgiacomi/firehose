package com.gltech.scale.ganglia;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

@Path("/monitor")
public class MonitorResource
{
	@Context
	private UriInfo uriInfo;

	@Path("/test")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response test()
	{
		return Response.ok("{}", MediaType.APPLICATION_JSON_TYPE).build();
	}
}
