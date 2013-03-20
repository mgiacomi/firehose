package com.gltech.scale.core.storage;

import com.gc.iotools.stream.is.InputStreamFromOutputStream;
import com.gltech.scale.core.model.ChannelMetaData;
import com.google.inject.Inject;

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
		return storage.get(channelName);
	}

	public void putChannelMetaData(ChannelMetaData channelMetaData)
	{
		storage.put(channelMetaData);
	}

	public InputStream getMessageStream(final String channelName, final String id)
	{
		return new InputStreamFromOutputStream<Long>()
		{
			@Override
			public Long produce(final OutputStream outputStream) throws Exception
			{
				storage.getMessages(channelName, id, outputStream);
				return 0L;
			}
		};
	}

	public byte[] get(String channelName, String id)
	{
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		storage.getMessages(channelName, id, byteArrayOutputStream);
		return byteArrayOutputStream.toByteArray();
	}

	public void put(String channelName, String id, InputStream inputStream)
	{
		storage.putMessages(channelName, id, inputStream, new HashMap<String, List<String>>());
	}

	public void put(String channelName, String id, byte[] payload)
	{
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(payload);
		put(channelName, id, byteArrayInputStream);
	}
}
