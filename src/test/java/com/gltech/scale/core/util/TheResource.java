package com.gltech.scale.core.util;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/TheResource/")
public class TheResource
{

	@GET
	@Consumes(MediaType.APPLICATION_JSON)
	public Response get()
	{
		return Response.ok().build();
	}
}
