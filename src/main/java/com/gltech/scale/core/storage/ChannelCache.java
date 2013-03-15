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
	private StorageServiceClient storageServiceClient;
	private ClusterService clusterService;

	@Inject
	public ChannelCache(ClusterService clusterService, StorageServiceClient storageServiceClient)
	{
		this.storageServiceClient = storageServiceClient;
		this.clusterService = clusterService;
	}

	public ChannelMetaData getChannelMetaData(String name, boolean createIfNotExist)
	{
		String key = "/channel/" + name;

		ChannelMetaData channelMetaData = bucketMetaDataCache.get(key);

		if (channelMetaData == null)
		{
			ServiceMetaData storageService = clusterService.getRegistrationService().getStorageServiceRoundRobin();
			ChannelMetaData newChannelMetaData = null;

			try
			{
				newChannelMetaData = storageServiceClient.getChannelMetaData(storageService, name);
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

				ChannelMetaData bmd = new ChannelMetaData(name, 60, false);

				try
				{
					storageServiceClient.putBucketMetaData(storageService, bmd);
				}
				catch (DuplicateBucketException dbe)
				{
					// It is fine if the bucket already exists.  Some thread beat us to it.
				}

				newChannelMetaData = storageServiceClient.getChannelMetaData(storageService, name);
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
