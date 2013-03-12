package com.gltech.scale.core.storage;

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

	public BucketMetaData getBucketMetaData(ServiceMetaData storageService, String customer, String bucket)
	{
		WebResource webResource = getResource(storageService, customer, bucket);
		ClientResponse response = webResource.get(ClientResponse.class);

		if (response.getStatus() == 404)
		{
			throw new Http404Exception("Bucket does not exist.");
		}

		if (response.getStatus() != 200)
		{
			throw new BucketMetaDataException("Failed : HTTP error code: " + response.getStatus());
		}

		return new BucketMetaData(customer, bucket, response.getEntity(String.class));
	}

	private WebResource getResource(ServiceMetaData storageService, String customer, String bucket)
	{
		String url = "http://" + storageService.getListenAddress() + ":" + storageService.getListenPort() + "/storage/" + UrlEncoder.encode(customer) + "/" + UrlEncoder.encode(bucket);
		return client.resource(url);
	}

	private WebResource getResource(ServiceMetaData storageService, String customer, String bucket, String id)
	{
		String url = "http://" + storageService.getListenAddress() + ":" + storageService.getListenPort() + "/storage/" + UrlEncoder.encode(customer) + "/" + UrlEncoder.encode(bucket) + "/" + UrlEncoder.encode(id);
		return client.resource(url);
	}

	public void putBucketMetaData(ServiceMetaData storageService, BucketMetaData bucketMetaData)
	{
		WebResource webResource = getResource(storageService, bucketMetaData.getCustomer(), bucketMetaData.getBucket());
		ClientResponse response = webResource.put(ClientResponse.class, bucketMetaData.toJson().toString());

		if (response.getStatus() == 403)
		{
			throw new DuplicateBucketException("Bucket already exists.");
		}

		if (response.getStatus() != 201)
		{
			throw new BucketMetaDataException("Failed : HTTP error code: " + response.getStatus());
		}
	}

	public InputStream getEventStream(ServiceMetaData storageService, String customer, String bucket, String id)
	{
		WebResource webResource = getResource(storageService, customer, bucket, id);
		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() == 404)
		{
			throw new Http404Exception("No events found for customer=" + customer + ", bucket=" + bucket + ", id=" + id);
		}

		if (response.getStatus() != 200)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}

		return response.getEntityInputStream();
	}

	public byte[] get(ServiceMetaData storageService, String customer, String bucket, String id)
	{
		WebResource webResource = getResource(storageService, customer, bucket, id);
		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() == 404)
		{
			throw new Http404Exception("No data found for customer=" + customer + ", bucket=" + bucket + ", id=" + id);
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

	public void put(ServiceMetaData storageService, String customer, String bucket, String id, InputStream inputStream)
	{
		WebResource webResource = getResource(storageService, customer, bucket, id);
		ClientResponse response = webResource.accept("application/json").put(ClientResponse.class, inputStream);

		if (response.getStatus() != 201)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}
	}

	public void put(ServiceMetaData storageService, String customer, String bucket, String id, byte[] payload)
	{
		WebResource webResource = getResource(storageService, customer, bucket, id);
		ClientResponse response = webResource.accept("application/json").put(ClientResponse.class, payload);

		if (response.getStatus() != 201)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}
	}
}
