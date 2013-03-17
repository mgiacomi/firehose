package com.gltech.scale.core.storage;

import com.gltech.scale.core.model.ChannelMetaData;
import com.google.inject.Inject;
import com.gltech.scale.core.cluster.ClusterService;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.util.Http404Exception;

import javax.ws.rs.core.MediaType;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ChannelCache
{
	static private ConcurrentMap<String, ChannelMetaData> bucketMetaDataCache = new ConcurrentHashMap<>();
	private StorageClient storageClient;
	private ClusterService clusterService;

	@Inject
	public ChannelCache(ClusterService clusterService, StorageClient storageClient)
	{
		this.storageClient = storageClient;
		this.clusterService = clusterService;
	}

	public ChannelMetaData getChannelMetaData(String name, boolean createIfNotExist)
	{
		String key = "/channel/" + name;

		ChannelMetaData channelMetaData = bucketMetaDataCache.get(key);

		if (channelMetaData == null)
		{
			ChannelMetaData newChannelMetaData = null;

			try
			{
				newChannelMetaData = storageClient.getChannelMetaData(name);
			}
			catch (Http404Exception e)
			{
				// It is ok to not find a record.
			}

			if (newChannelMetaData == null)
			{
				if (!createIfNotExist)
				{
					return null;
				}

				ChannelMetaData bmd = new ChannelMetaData(name, ChannelMetaData.TTL_MONTH, false);

				storageClient.putBucketMetaData(bmd);

				newChannelMetaData = storageClient.getChannelMetaData(name);
			}

			channelMetaData = bucketMetaDataCache.putIfAbsent(key, newChannelMetaData);
			if (channelMetaData == null)
			{
				channelMetaData = newChannelMetaData;
			}
		}

		return channelMetaData;
	}
}
