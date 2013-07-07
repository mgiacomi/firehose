package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.model.BatchMetaData;
import com.gltech.scale.core.model.Message;
import com.gltech.scale.util.RestClientCreator;
import com.gltech.scale.core.model.ModelIO;
import com.google.inject.Inject;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.io.InputStream;

public class AggregatorRestClient
{
	private static final Logger logger = LoggerFactory.getLogger(AggregatorRestClient.class);
	private final Client client = RestClientCreator.createCached();
	private ModelIO modelIO;

	@Inject
	public AggregatorRestClient(ModelIO modelIO)
	{
		this.modelIO = modelIO;
	}

	public void postMessage(ServiceMetaData aggregator, String channelName, DateTime dateTime, Message message)
	{
		String url = "http://" + aggregator.getListenAddress() + ":" + aggregator.getListenPort() + "/aggregator/message/" + channelName + "/" + dateTime.toString("YYYY/MM/dd/HH/mm/ss");
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.type(MediaType.APPLICATION_OCTET_STREAM_TYPE).post(ClientResponse.class, modelIO.toBytes(message));

		if (response.getStatus() != 202)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus() +" - "+ url);
		}
	}

	public void postBackupMessage(ServiceMetaData aggregator, String channelName, DateTime dateTime, Message message)
	{
		String url = "http://" + aggregator.getListenAddress() + ":" + aggregator.getListenPort() + "/aggregator/backup/message/" + channelName + "/" + dateTime.toString("YYYY/MM/dd/HH/mm/ss");
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.type(MediaType.APPLICATION_OCTET_STREAM_TYPE).post(ClientResponse.class, modelIO.toBytes(message));

		if (response.getStatus() != 202)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus() +" - "+ url);
		}
	}

	public InputStream getBatchMessagesStream(ServiceMetaData aggregator, String channelName, DateTime dateTime)
	{
		try
		{
			String url = "http://" + aggregator.getListenAddress() + ":" + aggregator.getListenPort() + "/aggregator/" + channelName + "/" + dateTime.toString("YYYY/MM/dd/HH/mm/ss") + "/messages";
			WebResource webResource = client.resource(url);
			ClientResponse response = webResource.accept(MediaType.APPLICATION_OCTET_STREAM_TYPE).get(ClientResponse.class);

			if (response.getStatus() != 200)
			{
				throw new RuntimeException("Failed : HTTP error code: " + response.getStatus() +" - "+ url);
			}

			return response.getEntityInputStream();
		}
		catch (RuntimeException e)
		{
			throw new RuntimeException("Failed to connect to Aggregator: " + aggregator + ", channelName=" + channelName + ", dateTime=" + dateTime, e);
		}
	}

	public InputStream getBackupBatchMessagesStream(ServiceMetaData aggregator, String channelName, DateTime dateTime)
	{
		String url = "http://" + aggregator.getListenAddress() + ":" + aggregator.getListenPort() + "/aggregator/" + channelName + "/" + dateTime.toString("YYYY/MM/dd/HH/mm/ss") + "/backup/messages";
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.accept(MediaType.APPLICATION_OCTET_STREAM_TYPE).get(ClientResponse.class);

		if (response.getStatus() != 200)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus() +" - "+ url);
		}

		return response.getEntityInputStream();
	}

	public void clearBatch(ServiceMetaData aggregator, String channelName, DateTime dateTime)
	{
		String url = "http://" + aggregator.getListenAddress() + ":" + aggregator.getListenPort() + "/aggregator/" + channelName + "/" + dateTime.toString("YYYY/MM/dd/HH/mm/ss") + "/clear";
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.accept(MediaType.APPLICATION_JSON_TYPE).delete(ClientResponse.class);

		if (response.getStatus() != 200)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus() +" - "+ url);
		}
	}

	public BatchMetaData getBatchMetaData(ServiceMetaData aggregator, String channelName, DateTime dateTime)
	{
		String url = "http://" + aggregator.getListenAddress() + ":" + aggregator.getListenPort() + "/aggregator/" + channelName + "/batch/" + dateTime.toString("YYYY/MM/dd/HH/mm/ss") + "/metadata";
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.accept(MediaType.APPLICATION_JSON_TYPE).get(ClientResponse.class);

		if (response.getStatus() != 200)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus() +" - "+ url);
		}

		return modelIO.toBatchMetaData(response.getEntity(String.class));
	}

	public BatchMetaData getBackupBatchMetaData(ServiceMetaData aggregator, String channelName, DateTime dateTime)
	{
		String url = "http://" + aggregator.getListenAddress() + ":" + aggregator.getListenPort() + "/aggregator/" + channelName + "/backup/batch/" + dateTime.toString("YYYY/MM/dd/HH/mm/ss") + "/metadata";
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.accept(MediaType.APPLICATION_JSON_TYPE).get(ClientResponse.class);

		if (response.getStatus() != 200)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus() +" - "+ url);
		}

		return modelIO.toBatchMetaData(response.getEntity(String.class));
	}
}
