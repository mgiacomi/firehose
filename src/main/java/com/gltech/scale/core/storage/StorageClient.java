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

public class StorageClient
{
	private Storage storage;

	@Inject
	public StorageClient(Storage storage)
	{
		this.storage = storage;
	}

	public ChannelMetaData getChannelMetaData(String channelName)
	{
		return storage.getBucket(channelName);
	}

	public void putBucketMetaData(ChannelMetaData channelMetaData)
	{
		storage.putBucket(channelMetaData);
	}

	public InputStream getEventStream(final String channelName, final String id)
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

	public byte[] get(String channelName, String id)
	{
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		storage.getPayload(channelName, id, byteArrayOutputStream);
		return byteArrayOutputStream.toByteArray();
	}

	public void put(String channelName, String id, InputStream inputStream)
	{
		storage.putPayload(channelName, id, inputStream, new HashMap<String, List<String>>());
	}

	public void put(String channelName, String id, byte[] payload)
	{
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(payload);
		put(channelName, id, byteArrayInputStream);
	}
}
