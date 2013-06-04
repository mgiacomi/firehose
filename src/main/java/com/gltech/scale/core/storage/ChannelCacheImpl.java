package com.gltech.scale.core.storage;

import com.gltech.scale.core.model.ChannelMetaData;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ChannelCacheImpl implements ChannelCache
{
	private static final Logger logger = LoggerFactory.getLogger(ChannelCacheImpl.class);
	static private ConcurrentMap<String, ChannelMetaData> channelMetaDataCache = new ConcurrentHashMap<>();
	private StorageClient storageClient;

	@Inject
	public ChannelCacheImpl(StorageClient storageClient)
	{
		this.storageClient = storageClient;
	}

	public ChannelMetaData getChannelMetaData(String name, boolean createIfNotExist)
	{
		String key = "/channel/" + name;

		ChannelMetaData channelMetaData = channelMetaDataCache.get(key);

		if (channelMetaData == null)
		{
			ChannelMetaData newChannelMetaData = storageClient.getChannelMetaData(name);

			if (newChannelMetaData == null)
			{
				if (!createIfNotExist)
				{
					return null;
				}

				try
				{
					storageClient.putChannelMetaData(new ChannelMetaData(name, ChannelMetaData.TTL_MONTH, false));
				}
				catch (DuplicateChannelException e)
				{
					logger.info("Channel {} has already been created.", name);
				}

				newChannelMetaData = storageClient.getChannelMetaData(name);
			}

			channelMetaData = channelMetaDataCache.putIfAbsent(key, newChannelMetaData);
			if (channelMetaData == null)
			{
				channelMetaData = newChannelMetaData;
			}
		}

		return channelMetaData;
	}
}
