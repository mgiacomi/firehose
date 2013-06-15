package com.gltech.scale.core.outbound;

import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.model.ModelIO;
import com.gltech.scale.util.ClientCreator;
import com.google.inject.Inject;
import com.sun.jersey.api.NotFoundException;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.joda.time.DateTime;

import javax.ws.rs.core.MediaType;
import java.util.concurrent.TimeUnit;

public class OutboundRestClient
{
	private final Client client = ClientCreator.createCached();
	private ModelIO modelIO;

	@Inject
	public OutboundRestClient(ModelIO modelIO)
	{
		this.modelIO = modelIO;
	}

	public ChannelMetaData getChannelMetaData(ServiceMetaData outboundService, String channelName)
	{
		String url = "http://" + outboundService.getListenAddress() + ":" + outboundService.getListenPort() + "/outbound/channel/" + channelName;
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.accept(MediaType.APPLICATION_JSON_TYPE).get(ClientResponse.class);

		if (response.getStatus() == 404)
		{
			throw new NotFoundException();
		}

		if (response.getStatus() != 200)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}

		return modelIO.toChannelMetaData(response.getEntity(String.class));
	}


	public String getMessages(ServiceMetaData outboundService, String channelName, DateTime dateTime, TimeUnit timeUnit)
	{
		String url = "http://" + outboundService.getListenAddress() + ":" + outboundService.getListenPort() + "/outbound/" + channelName + "/";

		if (timeUnit.equals(TimeUnit.SECONDS))
		{
			url = url + dateTime.toString("YYYY/MM/dd/HH/mm/ss");
		}
		else if (timeUnit.equals(TimeUnit.MINUTES))
		{
			url = url + dateTime.toString("YYYY/MM/dd/HH/mm");
		}
		else if (timeUnit.equals(TimeUnit.HOURS))
		{
			url = url + dateTime.toString("YYYY/MM/dd/HH");
		}
		else if (timeUnit.equals(TimeUnit.DAYS))
		{
			url = url + dateTime.toString("YYYY/MM/dd");
		}
		else
		{
			throw new IllegalArgumentException("TimeUnit must be SECONDS, MINUTES, HOURS, DAYS");
		}
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.accept(MediaType.APPLICATION_JSON_TYPE).get(ClientResponse.class);

		if (response.getStatus() != 200)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus() + " for url: " + url);
		}

		return response.getEntity(String.class);
	}

}
