package com.gltech.scale.core.inbound;

import com.gltech.scale.core.coordination.registration.ServiceMetaData;
import com.gltech.scale.core.storage.BucketMetaData;
import com.gltech.scale.core.util.ClientCreator;
import com.gltech.scale.core.util.Http404Exception;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.joda.time.DateTime;

import java.util.concurrent.TimeUnit;

public class InboundRestClient
{
	private final Client client = ClientCreator.createCached();

	public BucketMetaData getBucketMetaData(ServiceMetaData eventService, String customer, String bucket)
	{
		String url = "http://" + eventService.getListenAddress() + ":" + eventService.getListenPort() + "/events/" + customer + "/" + bucket;
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() == 404)
		{
			throw new Http404Exception("Bucket does not exist.");
		}

		if (response.getStatus() != 200)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}

		return new BucketMetaData(customer, bucket, response.getEntity(String.class));
	}

	public void postEvent(ServiceMetaData eventService, String customer, String bucket, String json)
	{
		String url = "http://" + eventService.getListenAddress() + ":" + eventService.getListenPort() + "/events/" + customer + "/" + bucket;
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.type("application/json").post(ClientResponse.class, json);

		if (response.getStatus() != 202)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response);
		}
	}

	public String getEvents(ServiceMetaData eventService, String customer, String bucket, DateTime dateTime, TimeUnit timeUnit)
	{
		String url = "http://" + eventService.getListenAddress() + ":" + eventService.getListenPort() + "/events/" + customer + "/" + bucket + "/";

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
		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() != 200)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus() + " for url: " + url);
		}

		return response.getEntity(String.class);
	}

}
