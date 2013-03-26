package com.gltech.scale.core.stats;

import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.stats.results.GroupStats;
import com.gltech.scale.util.ClientCreator;
import com.google.common.base.Throwables;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.List;

public class StatsRestClient
{
	private final Client client = ClientCreator.createCached();
	private Schema<GroupStats> groupStatsSchema = RuntimeSchema.getSchema(GroupStats.class);

	public List<GroupStats> getGroupStats(ServiceMetaData inboundService)
	{
		String url = "http://" + inboundService.getListenAddress() + ":" + inboundService.getListenPort() + "/inbound/stats";
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.accept(MediaType.APPLICATION_OCTET_STREAM_TYPE).get(ClientResponse.class);

		if (response.getStatus() != 200)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response);
		}

		try
		{
			return ProtostuffIOUtil.parseListFrom(response.getEntityInputStream(), groupStatsSchema);
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}
}
