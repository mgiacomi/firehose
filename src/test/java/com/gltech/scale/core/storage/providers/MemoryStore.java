package com.gltech.scale.core.storage.providers;

import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.storage.Storage;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public class MemoryStore implements Storage
{
	public void putBucket(ChannelMetaData channelMetaData)
	{
	}

	public ChannelMetaData getBucket(String channelName)
	{
		return null;
	}

	public void putPayload(String channelName, String id, InputStream inputStream, Map<String, List<String>> headers)
	{
	}

	public void getPayload(String cchannelName, String id, OutputStream outputStream)
	{
	}
}
