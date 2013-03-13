package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.model.BatchMetaData;
import com.gltech.scale.core.model.Message;
import com.gltech.scale.core.model.Batch;
import com.gltech.scale.util.ClientCreator;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.List;

public class AggregatorRestClient
{
	private static final Logger logger = LoggerFactory.getLogger(AggregatorRestClient.class);
	private final Client client = ClientCreator.createCached();

	public void postEvent(ServiceMetaData aggregator, Message event)
	{
		String url = "http://" + aggregator.getListenAddress() + ":" + aggregator.getListenPort() + "/aggregator/event";
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.type("application/json").post(ClientResponse.class, event.toJson().toString());

		if (response.getStatus() != 202)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}
	}

	public void postBackupEvent(ServiceMetaData aggregator, Message event)
	{
		String url = "http://" + aggregator.getListenAddress() + ":" + aggregator.getListenPort() + "/aggregator/backup/event";
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.type("application/json").post(ClientResponse.class, event.toJson().toString());

		if (response.getStatus() != 202)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}
	}

	public List<Message> getTimeBucketEvents(ServiceMetaData aggregator, String customer, String bucket, DateTime dateTime)
	{
		String url = "http://" + aggregator.getListenAddress() + ":" + aggregator.getListenPort() + "/aggregator/" + customer + "/" + bucket + "/" + dateTime.toString("YYYY/MM/dd/HH/mm/ss") + "/timebucket/events";
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() != 200)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}

		return Batch.jsonToEvents(response.getEntityInputStream());
	}

	public InputStream getTimeBucketEventsStream(ServiceMetaData aggregator, String customer, String bucket, DateTime dateTime)
	{
		try
		{
			String url = "http://" + aggregator.getListenAddress() + ":" + aggregator.getListenPort() + "/aggregator/" + customer + "/" + bucket + "/" + dateTime.toString("YYYY/MM/dd/HH/mm/ss") + "/timebucket/events";
			WebResource webResource = client.resource(url);
			ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

			if (response.getStatus() != 200)
			{
				throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
			}

			return response.getEntityInputStream();
		}
		catch (RuntimeException e)
		{
			throw new RuntimeException("Failed to connect to Aggregator: " + aggregator + ", customer=" + customer + ", bucket=" + bucket + ", dateTime=" + dateTime, e);
		}
	}

	public List<Message> getBackupTimeBucketEvents(ServiceMetaData aggregator, String customer, String bucket, DateTime dateTime)
	{
		String url = "http://" + aggregator.getListenAddress() + ":" + aggregator.getListenPort() + "/aggregator/" + customer + "/" + bucket + "/" + dateTime.toString("YYYY/MM/dd/HH/mm/ss") + "/backup/timebucket/events";
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() != 200)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}

		return Batch.jsonToEvents(response.getEntityInputStream());
	}

	public InputStream getBackupTimeBucketEventsStream(ServiceMetaData aggregator, String customer, String bucket, DateTime dateTime)
	{
		String url = "http://" + aggregator.getListenAddress() + ":" + aggregator.getListenPort() + "/aggregator/" + customer + "/" + bucket + "/" + dateTime.toString("YYYY/MM/dd/HH/mm/ss") + "/backup/timebucket/events";
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() != 200)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}

		return response.getEntityInputStream();
	}

	public void clearTimeBucket(ServiceMetaData aggregator, String customer, String bucket, DateTime dateTime)
	{
		String url = "http://" + aggregator.getListenAddress() + ":" + aggregator.getListenPort() + "/aggregator/" + customer + "/" + bucket + "/" + dateTime.toString("YYYY/MM/dd/HH/mm/ss") + "/clear";
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.accept("application/json").delete(ClientResponse.class);

		if (response.getStatus() != 200)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}
	}

	public BatchMetaData getTimeBucketMetaData(ServiceMetaData aggregator, String customer, String bucket, DateTime dateTime)
	{
		String url = "http://" + aggregator.getListenAddress() + ":" + aggregator.getListenPort() + "/aggregator/" + customer + "/" + bucket + "/timebucket/" + dateTime.toString("YYYY/MM/dd/HH/mm/ss") + "/metadata";
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() != 200)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}

		return new BatchMetaData(response.getEntity(String.class));
	}

	public BatchMetaData getBackupTimeBucketMetaData(ServiceMetaData aggregator, String customer, String bucket, DateTime dateTime)
	{
		String url = "http://" + aggregator.getListenAddress() + ":" + aggregator.getListenPort() + "/aggregator/" + customer + "/" + bucket + "/backup/timebucket/" + dateTime.toString("YYYY/MM/dd/HH/mm/ss") + "/metadata";
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() != 200)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}

		return new BatchMetaData(response.getEntity(String.class));
	}
}
