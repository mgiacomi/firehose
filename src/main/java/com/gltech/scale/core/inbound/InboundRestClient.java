package com.gltech.scale.core.inbound;

import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.storage.BucketMetaDataException;
import com.gltech.scale.core.storage.DuplicateChannelException;
import com.gltech.scale.util.RestClientCreator;
import com.gltech.scale.core.model.ModelIO;
import com.google.inject.Inject;
import com.sun.jersey.api.NotFoundException;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.joda.time.DateTime;

import javax.ws.rs.core.MediaType;
import java.util.concurrent.TimeUnit;

public class InboundRestClient
{
	private final Client client = RestClientCreator.createCached();
	private ModelIO modelIO;

	@Inject
	public InboundRestClient(ModelIO modelIO)
	{
		this.modelIO = modelIO;
	}

	public ChannelMetaData getChannelMetaData(ServiceMetaData inboundService, String channelName)
	{
		String url = "http://" + inboundService.getListenAddress() + ":" + inboundService.getListenPort() + "/inbound/channel/" + channelName;
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

	public void putChannelMetaData(ServiceMetaData inboundService, ChannelMetaData channelMetaData)
	{
		String url = "http://" + inboundService.getListenAddress() + ":" + inboundService.getListenPort() + "/inbound/channel/new";
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.type(MediaType.APPLICATION_JSON_TYPE).put(ClientResponse.class, modelIO.toJson(channelMetaData));

		if (response.getStatus() == 403)
		{
			throw new DuplicateChannelException("Bucket already exists.");
		}

		if (response.getStatus() != 201)
		{
			throw new BucketMetaDataException("Failed : HTTP error code: " + response.getStatus());
		}
	}

	public void postMessage(ServiceMetaData inboundService, String channelName, String json)
	{
		String url = "http://" + inboundService.getListenAddress() + ":" + inboundService.getListenPort() + "/inbound/" + channelName;
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.type(MediaType.APPLICATION_JSON_TYPE).post(ClientResponse.class, json);

		if (response.getStatus() != 202)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response);
		}
	}

	public String getMessages(ServiceMetaData inboundService, String channelName, DateTime dateTime, TimeUnit timeUnit)
	{
		String url = "http://" + inboundService.getListenAddress() + ":" + inboundService.getListenPort() + "/inbound/" + channelName + "/";

		if (timeUnit.equals(TimeUnit.SECONDS))
		{
			url = url + dateTime.toString("yyyy/MM/dd/HH/mm/ss");
		}
		else if (timeUnit.equals(TimeUnit.MINUTES))
		{
			url = url + dateTime.toString("yyyy/MM/dd/HH/mm");
		}
		else if (timeUnit.equals(TimeUnit.HOURS))
		{
			url = url + dateTime.toString("yyyy/MM/dd/HH");
		}
		else if (timeUnit.equals(TimeUnit.DAYS))
		{
			url = url + dateTime.toString("yyyy/MM/dd");
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
