package com.gltech.scale.core.storage.providers;

import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.storage.DuplicateChannelException;
import com.gltech.scale.core.storage.Storage;
import com.google.common.base.Throwables;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryStore implements Storage
{
	private static final Logger logger = LoggerFactory.getLogger(MemoryStore.class);

	private ConcurrentHashMap<String, ChannelMetaData> channelMap = new ConcurrentHashMap<>();

	private ConcurrentHashMap<ChannelMetaData, ConcurrentHashMap<String, byte[]>> payloadMap =
			new ConcurrentHashMap<>();

	@Override
	public void put(ChannelMetaData channelMetaData)
	{
		logger.debug("Adding ChannelMetaData for {}", channelMetaData.getName());
		if (channelMap.containsKey(channelMetaData.getName()))
		{
			throw new DuplicateChannelException("Channel already exists " + channelMetaData.getName());
		}

		channelMap.put(channelMetaData.getName(), channelMetaData);
		payloadMap.putIfAbsent(channelMetaData, new ConcurrentHashMap<String, byte[]>());
	}

	@Override
	public ChannelMetaData get(String channelName)
	{
		return channelMap.get(channelName);
	}

	@Override
	public void putMessages(String channelName, String id, InputStream inputStream, Map<String, List<String>> headers)
	{
		try
		{
			byte[] bytes = IOUtils.toByteArray(inputStream);
			logger.debug("putting payload channel={}, key={}, length={}", channelName, id, bytes.length);
			ChannelMetaData channelMetaData = channelMap.get(channelName);
			payloadMap.get(channelMetaData).put(id, bytes);
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}

	@Override
	public void getMessages(String channelName, String id, OutputStream outputStream)
	{
		try
		{
			ChannelMetaData channelMetaData = channelMap.get(channelName);
			byte[] bytes = payloadMap.get(channelMetaData).get(id);

			if (bytes != null)
			{
				logger.info("Reading from memory store channelName={} id={} size={}", channelName, id, bytes.length);
				outputStream.write(bytes);
			}
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}
}
