package com.gltech.scale.core.storage;

import com.gltech.scale.core.model.ChannelMetaData;
import com.google.inject.Inject;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ChannelCacheImpl implements ChannelCache
{
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

				storageClient.putChannelMetaData(new ChannelMetaData(name, ChannelMetaData.TTL_MONTH, false));

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
