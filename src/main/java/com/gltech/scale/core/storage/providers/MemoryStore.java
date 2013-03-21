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
import java.util.concurrent.ConcurrentHashMap;

public class MemoryStore implements Storage
{
	private static final Logger logger = LoggerFactory.getLogger(MemoryStore.class);

	private ConcurrentHashMap<String, ChannelMetaData> channelMap = new ConcurrentHashMap<>();

	private ConcurrentHashMap<ChannelMetaData, ConcurrentHashMap<String, byte[]>> payloadMap =
			new ConcurrentHashMap<>();

	@Override
	public void putChannelMetaData(ChannelMetaData channelMetaData)
	{
		logger.debug("Adding ChannelMetaData for {}", channelMetaData);
		if (channelMap.containsKey(channelMetaData.getName()))
		{
			throw new DuplicateChannelException("Channel already exists " + channelMetaData.getName());
		}

		channelMap.put(channelMetaData.getName(), channelMetaData);
		payloadMap.putIfAbsent(channelMetaData, new ConcurrentHashMap<String, byte[]>());
	}

	@Override
	public ChannelMetaData getChannelMetaData(String channelName)
	{
		return channelMap.get(channelName);
	}


	public void putBytes(ChannelMetaData channelMetaData, String id, byte[] data)
	{
	}

	public byte[] getBytes(ChannelMetaData channelMetaData, String id)
	{
		return new byte[0];
	}

	@Override
	public void putMessages(ChannelMetaData channelMetaData, String id, InputStream inputStream)
	{
		try
		{
			byte[] bytes = IOUtils.toByteArray(inputStream);
			logger.debug("putting payload channel={}, key={}, length={}", channelMetaData.getName(), id, bytes.length);
			payloadMap.get(channelMetaData).put(id, bytes);
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}

	@Override
	public void getMessages(ChannelMetaData channelMetaData, String id, OutputStream outputStream)
	{
		try
		{
			byte[] bytes = payloadMap.get(channelMetaData).get(id);

			if (bytes != null)
			{
				logger.info("Reading from memory store channelName={} id={} size={}", channelMetaData.getName(), id, bytes.length);
				outputStream.write(bytes);
			}
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}
}
