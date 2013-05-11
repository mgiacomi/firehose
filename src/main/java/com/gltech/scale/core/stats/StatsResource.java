package com.gltech.scale.core.stats;

import com.gltech.scale.core.aggregator.Aggregator;
import com.gltech.scale.core.model.Batch;
import com.gltech.scale.monitoring.model.ResultsIO;
import com.gltech.scale.monitoring.model.ServerStats;
import com.gltech.scale.util.Props;
import com.google.inject.Inject;
import com.sun.istack.internal.Nullable;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.*;

@Path("/stats")
public class StatsResource
{
	@Inject(optional = true)
	private Aggregator aggregator;
	private StatsManager statsManager;
	private ResultsIO resultsIO;
	private Props props = Props.getProps();

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
		ServerStats serverStats = statsManager.getServerStats();

		if (props.get("enable.aggregator", false))
		{
			for (Batch batch : aggregator.getActiveBatches())
			{
				serverStats.getActiveBatches().add(batch.getMetaData());
			}

			for (Batch batch : aggregator.getActiveBackupBatches())
			{
				serverStats.getActiveBackupBatches().add(batch.getMetaData());
			}
		}

		String json = resultsIO.toJson(serverStats);
		return Response.ok(json, MediaType.APPLICATION_JSON).build();
	}

	@GET
	@Path("/all")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public Response getProtoStats()
	{
		ServerStats serverStats = statsManager.getServerStats();

		if (props.get("enable.aggregator", false))
		{
			for (Batch batch : aggregator.getActiveBatches())
			{
				serverStats.getActiveBatches().add(batch.getMetaData());
			}

			for (Batch batch : aggregator.getActiveBackupBatches())
			{
				serverStats.getActiveBackupBatches().add(batch.getMetaData());
			}
		}

		byte[] bytes = resultsIO.toBytes(serverStats);
		return Response.ok(bytes, MediaType.APPLICATION_OCTET_STREAM).build();
	}
}
