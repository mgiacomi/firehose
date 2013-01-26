package com.gltech.scale.core.storage;

import com.gc.iotools.stream.is.InputStreamFromOutputStream;
import com.google.inject.Inject;
import com.gltech.scale.core.coordination.registration.ServiceMetaData;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;

public class StorageServiceLocalClient implements StorageServiceClient
{
	private Storage storage;

	@Inject
	public StorageServiceLocalClient(Storage storage)
	{
		this.storage = storage;
	}

	public BucketMetaData getBucketMetaData(ServiceMetaData storageService, String customer, String bucket)
	{
		return storage.getBucket(customer, bucket);
	}

	public void putBucketMetaData(ServiceMetaData storageService, BucketMetaData bucketMetaData)
	{
		storage.putBucket(bucketMetaData);
	}

	public InputStream getEventStream(ServiceMetaData storageService, final String customer, final String bucket, final String id)
	{
		return new InputStreamFromOutputStream<Long>()
		{
			@Override
			public Long produce(final OutputStream outputStream) throws Exception
			{
				storage.getPayload(customer, bucket, id, outputStream);
				return 0L;
			}
		};
	}

	public byte[] get(ServiceMetaData storageService, String customer, String bucket, String id)
	{
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		storage.getPayload(customer, bucket, id, byteArrayOutputStream);
		return byteArrayOutputStream.toByteArray();
	}

	public void put(ServiceMetaData storageService, String customer, String bucket, String id, InputStream inputStream)
	{
		storage.putPayload(customer, bucket, id, inputStream, new HashMap<String, List<String>>());
	}

	public void put(ServiceMetaData storageService, String customer, String bucket, String id, byte[] payload)
	{
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(payload);
		put(storageService, customer, bucket, id, byteArrayInputStream);
	}
}
