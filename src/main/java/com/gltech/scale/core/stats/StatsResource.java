package com.gltech.scale.core.stats;

import com.gltech.scale.monitoring.model.ResultsIO;
import com.google.inject.Inject;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.*;

@Path("/stats")
public class StatsResource
{
	private StatsManager statsManager;
	private ResultsIO resultsIO;

	@Context
	private HttpHeaders httpHeaders;

	@Context
	private UriInfo uriInfo;

	@Inject
	public StatsResource(StatsManager statsManager, ResultsIO resultsIO)
	{
		this.statsManager = statsManager;
		this.resultsIO = resultsIO;
	}

	@GET
	@Path("/all")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getJsonStats()
	{
		String json = resultsIO.toJson(statsManager.getServerStats());
		return Response.ok(json, MediaType.APPLICATION_JSON).build();
	}

	@GET
	@Path("/all")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public Response getProtoStats()
	{
		byte[] bytes = resultsIO.toBytes(statsManager.getServerStats());
		return Response.ok(bytes, MediaType.APPLICATION_OCTET_STREAM).build();
	}
}
