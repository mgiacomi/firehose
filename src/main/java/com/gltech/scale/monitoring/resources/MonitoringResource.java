package com.gltech.scale.monitoring.resources;

import com.gltech.scale.monitoring.services.ClusterStatsService;
import com.google.inject.Inject;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.*;

@Path("/monitoring")
public class MonitoringResource
{
	private ClusterStatsService clusterStatsService;

	@Context
	private HttpHeaders httpHeaders;

	@Context
	private UriInfo uriInfo;

	@Inject
	public MonitoringResource(ClusterStatsService clusterStatsService)
	{
		this.clusterStatsService = clusterStatsService;
	}

	@GET
	@Path("/all")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getJsonStats()
	{
		return Response.ok(clusterStatsService.getJsonStatsAll(), MediaType.APPLICATION_JSON).build();
	}
}