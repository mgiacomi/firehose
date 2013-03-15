package com.gltech.scale.core.storage;

import com.gltech.scale.core.model.ChannelMetaData;
import com.google.common.base.Throwables;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.util.ClientCreator;
import com.gltech.scale.util.Http404Exception;
import com.gltech.scale.util.UrlEncoder;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;

public class StorageServiceRestClient implements StorageServiceClient
{
	private final Client client = ClientCreator.createCached();

	public ChannelMetaData getChannelMetaData(ServiceMetaData storageService, String channelName)
	{
		WebResource webResource = getResource(storageService, channelName);
		ClientResponse response = webResource.get(ClientResponse.class);

		if (response.getStatus() == 404)
		{
			throw new Http404Exception("Bucket does not exist.");
		}

		if (response.getStatus() != 200)
		{
			throw new BucketMetaDataException("Failed : HTTP error code: " + response.getStatus());
		}

//		return new ChannelMetaData(channelName, response.getEntity(String.class));
return null;
	}

	private WebResource getResource(ServiceMetaData storageService, String channelName)
	{
		String url = "http://" + storageService.getListenAddress() + ":" + storageService.getListenPort() + "/storage/" + UrlEncoder.encode(channelName);
		return client.resource(url);
	}

	private WebResource getResource(ServiceMetaData storageService, String channelName, String id)
	{
		String url = "http://" + storageService.getListenAddress() + ":" + storageService.getListenPort() + "/storage/" + UrlEncoder.encode(channelName) + "/" + UrlEncoder.encode(id);
		return client.resource(url);
	}

	public void putBucketMetaData(ServiceMetaData storageService, ChannelMetaData channelMetaData)
	{
		WebResource webResource = getResource(storageService, channelMetaData.getName());
//		ClientResponse response = webResource.put(ClientResponse.class, channelMetaData.toJson().toString());
ClientResponse response = null;

		if (response.getStatus() == 403)
		{
			throw new DuplicateBucketException("Bucket already exists.");
		}

		if (response.getStatus() != 201)
		{
			throw new BucketMetaDataException("Failed : HTTP error code: " + response.getStatus());
		}
	}

	public InputStream getEventStream(ServiceMetaData storageService, String channelName, String id)
	{
		WebResource webResource = getResource(storageService, channelName, id);
		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() == 404)
		{
			throw new Http404Exception("No events found for channelName=" + channelName + ", id=" + id);
		}

		if (response.getStatus() != 200)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}

		return response.getEntityInputStream();
	}

	public byte[] get(ServiceMetaData storageService, String channelName, String id)
	{
		WebResource webResource = getResource(storageService, channelName, id);
		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() == 404)
		{
			throw new Http404Exception("No data found for channelName=" + channelName + ", id=" + id);
		}

		if (response.getStatus() != 200)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}

		try
		{
			return IOUtils.toByteArray(response.getEntityInputStream());
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}

	public void put(ServiceMetaData storageService, String channelName, String id, InputStream inputStream)
	{
		WebResource webResource = getResource(storageService, channelName, id);
		ClientResponse response = webResource.accept("application/json").put(ClientResponse.class, inputStream);

		if (response.getStatus() != 201)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}
	}

	public void put(ServiceMetaData storageService, String channelName, String id, byte[] payload)
	{
		WebResource webResource = getResource(storageService, channelName, id);
		ClientResponse response = webResource.accept("application/json").put(ClientResponse.class, payload);

		if (response.getStatus() != 201)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}
	}
}
