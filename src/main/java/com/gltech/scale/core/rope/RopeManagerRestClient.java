package com.gltech.scale.core.rope;

import com.gltech.scale.core.coordination.registration.ServiceMetaData;
import com.gltech.scale.core.event.EventPayload;
import com.gltech.scale.core.util.ClientCreator;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.List;

public class RopeManagerRestClient
{
	private static final Logger logger = LoggerFactory.getLogger("com.lokiscale.rope.RopeManagerRestClient");
	private final Client client = ClientCreator.createCached();

	public void postEvent(ServiceMetaData ropeManager, EventPayload event)
	{
		String url = "http://" + ropeManager.getListenAddress() + ":" + ropeManager.getListenPort() + "/ropes/event";
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.type("application/json").post(ClientResponse.class, event.toJson().toString());

		if (response.getStatus() != 202)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}
	}

	public void postBackupEvent(ServiceMetaData ropeManager, EventPayload event)
	{
		String url = "http://" + ropeManager.getListenAddress() + ":" + ropeManager.getListenPort() + "/ropes/backup/event";
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.type("application/json").post(ClientResponse.class, event.toJson().toString());

		if (response.getStatus() != 202)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}
	}

	public List<EventPayload> getTimeBucketEvents(ServiceMetaData ropeManager, String customer, String bucket, DateTime dateTime)
	{
		String url = "http://" + ropeManager.getListenAddress() + ":" + ropeManager.getListenPort() + "/ropes/" + customer + "/" + bucket + "/" + dateTime.toString("YYYY/MM/dd/HH/mm/ss") + "/timebucket/events";
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() != 200)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}

		return TimeBucket.jsonToEvents(response.getEntityInputStream());
	}

	public InputStream getTimeBucketEventsStream(ServiceMetaData ropeManager, String customer, String bucket, DateTime dateTime)
	{
		try
		{
			String url = "http://" + ropeManager.getListenAddress() + ":" + ropeManager.getListenPort() + "/ropes/" + customer + "/" + bucket + "/" + dateTime.toString("YYYY/MM/dd/HH/mm/ss") + "/timebucket/events";
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
			throw new RuntimeException("Failed to connect to RopeManager: " + ropeManager + ", customer=" + customer + ", bucket=" + bucket + ", dateTime=" + dateTime, e);
		}
	}

	public List<EventPayload> getBackupTimeBucketEvents(ServiceMetaData ropeManager, String customer, String bucket, DateTime dateTime)
	{
		String url = "http://" + ropeManager.getListenAddress() + ":" + ropeManager.getListenPort() + "/ropes/" + customer + "/" + bucket + "/" + dateTime.toString("YYYY/MM/dd/HH/mm/ss") + "/backup/timebucket/events";
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() != 200)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}

		return TimeBucket.jsonToEvents(response.getEntityInputStream());
	}

	public InputStream getBackupTimeBucketEventsStream(ServiceMetaData ropeManager, String customer, String bucket, DateTime dateTime)
	{
		String url = "http://" + ropeManager.getListenAddress() + ":" + ropeManager.getListenPort() + "/ropes/" + customer + "/" + bucket + "/" + dateTime.toString("YYYY/MM/dd/HH/mm/ss") + "/backup/timebucket/events";
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() != 200)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}

		return response.getEntityInputStream();
	}

	public void clearTimeBucket(ServiceMetaData ropeManager, String customer, String bucket, DateTime dateTime)
	{
		String url = "http://" + ropeManager.getListenAddress() + ":" + ropeManager.getListenPort() + "/ropes/" + customer + "/" + bucket + "/" + dateTime.toString("YYYY/MM/dd/HH/mm/ss") + "/clear";
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.accept("application/json").delete(ClientResponse.class);

		if (response.getStatus() != 200)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}
	}

	public TimeBucketMetaData getTimeBucketMetaData(ServiceMetaData ropeManager, String customer, String bucket, DateTime dateTime)
	{
		String url = "http://" + ropeManager.getListenAddress() + ":" + ropeManager.getListenPort() + "/ropes/" + customer + "/" + bucket + "/timebucket/" + dateTime.toString("YYYY/MM/dd/HH/mm/ss") + "/metadata";
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() != 200)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}

		return new TimeBucketMetaData(response.getEntity(String.class));
	}

	public TimeBucketMetaData getBackupTimeBucketMetaData(ServiceMetaData ropeManager, String customer, String bucket, DateTime dateTime)
	{
		String url = "http://" + ropeManager.getListenAddress() + ":" + ropeManager.getListenPort() + "/ropes/" + customer + "/" + bucket + "/backup/timebucket/" + dateTime.toString("YYYY/MM/dd/HH/mm/ss") + "/metadata";
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() != 200)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}

		return new TimeBucketMetaData(response.getEntity(String.class));
	}
}
