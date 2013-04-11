package com.gltech.scale.core.stats;

import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.monitoring.model.ResultsIO;
import com.gltech.scale.monitoring.model.ServerStats;
import com.gltech.scale.util.ClientCreator;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.commons.io.IOUtils;

import javax.ws.rs.core.MediaType;
import java.io.IOException;

public class StatsRestClient
{
	private final Client client = ClientCreator.createCached();
	private ResultsIO resultsIO;

	@Inject
	public StatsRestClient(ResultsIO resultsIO)
	{
		this.resultsIO = resultsIO;
	}

	public ServerStats getServerStats(ServiceMetaData serverService)
	{
		String url = "http://" + serverService.getListenAddress() + ":" + serverService.getListenPort() + "/stats/all";
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.accept(MediaType.APPLICATION_OCTET_STREAM_TYPE).get(ClientResponse.class);

		if (response.getStatus() != 200)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response);
		}

		try
		{
			return resultsIO.toServerStats(IOUtils.toByteArray(response.getEntityInputStream()));
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}
}
