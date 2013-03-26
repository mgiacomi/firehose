package com.gltech.scale.core.stats;

import com.google.inject.Inject;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.*;

@Path("/stats")
public class StatsResource
{
	private StatsManager statsManager;

	@Context
	private HttpHeaders httpHeaders;

	@Context
	private UriInfo uriInfo;

	@Inject
	public StatsResource(StatsManager statsManager)
	{
		this.statsManager = statsManager;
	}

	@GET
	@Path("/all")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public Response getProtoStats()
	{
		return Response.ok(statsManager.toBytes(), MediaType.APPLICATION_OCTET_STREAM).build();
	}

	@GET
	@Path("/all")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getJsonStats()
	{
		return Response.ok(statsManager.toJson(), MediaType.APPLICATION_JSON).build();
	}
}
