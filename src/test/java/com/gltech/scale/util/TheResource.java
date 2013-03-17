package com.gltech.scale.util;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/TheResource/")
public class TheResource
{

	@GET
	@Path("/test")
	@Produces(MediaType.APPLICATION_JSON)
	public Response get()
	{
		return Response.ok("{}", MediaType.APPLICATION_JSON_TYPE).build();
	}
}
