package com.gltech.scale.core.storage;

import com.gc.iotools.stream.is.InputStreamFromOutputStream;
import com.gltech.scale.core.model.ChannelMetaData;
import com.google.inject.Inject;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;

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

	public ChannelMetaData getChannelMetaData(ServiceMetaData storageService, String channelName)
	{
		return storage.getBucket(channelName);
	}

	public void putBucketMetaData(ServiceMetaData storageService, ChannelMetaData channelMetaData)
	{
		storage.putBucket(channelMetaData);
	}

	public InputStream getEventStream(ServiceMetaData storageService, final String channelName, final String id)
	{
		return new InputStreamFromOutputStream<Long>()
		{
			@Override
			public Long produce(final OutputStream outputStream) throws Exception
			{
				storage.getPayload(channelName, id, outputStream);
				return 0L;
			}
		};
	}

	public byte[] get(ServiceMetaData storageService, String channelName, String id)
	{
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		storage.getPayload(channelName, id, byteArrayOutputStream);
		return byteArrayOutputStream.toByteArray();
	}

	public void put(ServiceMetaData storageService, String channelName, String id, InputStream inputStream)
	{
		storage.putPayload(channelName, id, inputStream, new HashMap<String, List<String>>());
	}

	public void put(ServiceMetaData storageService, String channelName, String id, byte[] payload)
	{
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(payload);
		put(storageService, channelName, id, byteArrayInputStream);
	}
}
