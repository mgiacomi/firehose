package com.gltech.scale.core.storage;

import com.gc.iotools.stream.is.InputStreamFromOutputStream;
import com.gltech.scale.core.model.ChannelMetaData;
import com.google.inject.Inject;

import java.io.InputStream;
import java.io.OutputStream;

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
		return storage.getChannelMetaData(channelName);
	}

	public void putChannelMetaData(ChannelMetaData channelMetaData)
	{
		storage.putChannelMetaData(channelMetaData);
	}

	public InputStream getMessageStream(final ChannelMetaData channelMetaData, final String id)
	{
		return new InputStreamFromOutputStream<Long>()
		{
			@Override
			public Long produce(final OutputStream outputStream) throws Exception
			{
				storage.getMessages(channelMetaData, id, outputStream);
				return 0L;
			}
		};
	}

	public byte[] getMessage(ChannelMetaData channelMetaData, String id)
	{
		return storage.getBytes(channelMetaData, id);
	}

	public void putMessages(ChannelMetaData channelMetaData, String id, InputStream inputStream)
	{
		storage.putMessages(channelMetaData, id, inputStream);
	}

	public void putMessage(ChannelMetaData channelMetaData, String id, byte[] data)
	{
		storage.putBytes(channelMetaData, id, data);
	}
}
