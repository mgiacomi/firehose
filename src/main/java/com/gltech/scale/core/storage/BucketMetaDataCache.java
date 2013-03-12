package com.gltech.scale.core.storage;

import com.gltech.scale.core.model.ChannelMetaData;
import com.google.inject.Inject;
import com.gltech.scale.core.cluster.ClusterService;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.util.Http404Exception;

import javax.ws.rs.core.MediaType;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class BucketMetaDataCache
{
	static private ConcurrentMap<String, ChannelMetaData> bucketMetaDataCache = new ConcurrentHashMap<>();
	private StorageServiceClient storageServiceClient;
	private ClusterService clusterService;

	@Inject
	public BucketMetaDataCache(ClusterService clusterService, StorageServiceClient storageServiceClient)
	{
		this.storageServiceClient = storageServiceClient;
		this.clusterService = clusterService;
	}

	public ChannelMetaData getBucketMetaData(String customer, String bucket, boolean createIfNotExist)
	{
		String key = "/" + customer + "/" + bucket;

		ChannelMetaData channelMetaData = bucketMetaDataCache.get(key);

		if (channelMetaData == null)
		{
			ServiceMetaData storageService = clusterService.getRegistrationService().getStorageServiceRoundRobin();
			ChannelMetaData newChannelMetaData = null;

			try
			{
				newChannelMetaData = storageServiceClient.getBucketMetaData(storageService, customer, bucket);
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

				ChannelMetaData bmd = new ChannelMetaData(customer, bucket, ChannelMetaData.BucketType.eventset, 60, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.medium, ChannelMetaData.Redundancy.singlewrite);

				try
				{
					storageServiceClient.putBucketMetaData(storageService, bmd);
				}
				catch (DuplicateBucketException dbe)
				{
					// It is fine if the bucket already exists.  Some thread beat us to it.
				}

				newChannelMetaData = storageServiceClient.getBucketMetaData(storageService, customer, bucket);
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
