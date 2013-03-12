package com.gltech.scale.core.storage;

import com.google.inject.Inject;
import com.gltech.scale.core.cluster.ClusterService;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.util.Http404Exception;

import javax.ws.rs.core.MediaType;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class BucketMetaDataCache
{
	static private ConcurrentMap<String, BucketMetaData> bucketMetaDataCache = new ConcurrentHashMap<>();
	private StorageServiceClient storageServiceClient;
	private ClusterService clusterService;

	@Inject
	public BucketMetaDataCache(ClusterService clusterService, StorageServiceClient storageServiceClient)
	{
		this.storageServiceClient = storageServiceClient;
		this.clusterService = clusterService;
	}

	public BucketMetaData getBucketMetaData(String customer, String bucket, boolean createIfNotExist)
	{
		String key = "/" + customer + "/" + bucket;

		BucketMetaData bucketMetaData = bucketMetaDataCache.get(key);

		if (bucketMetaData == null)
		{
			ServiceMetaData storageService = clusterService.getRegistrationService().getStorageServiceRoundRobin();
			BucketMetaData newBucketMetaData = null;

			try
			{
				newBucketMetaData = storageServiceClient.getBucketMetaData(storageService, customer, bucket);
			}
			catch (Http404Exception e)
			{
				// It is ok to not find a record.
			}

			if (newBucketMetaData == null)
			{
				if (!createIfNotExist)
				{
					return null;
				}

				BucketMetaData bmd = new BucketMetaData(customer, bucket, BucketMetaData.BucketType.eventset, 60, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.medium, BucketMetaData.Redundancy.singlewrite);

				try
				{
					storageServiceClient.putBucketMetaData(storageService, bmd);
				}
				catch (DuplicateBucketException dbe)
				{
					// It is fine if the bucket already exists.  Some thread beat us to it.
				}

				newBucketMetaData = storageServiceClient.getBucketMetaData(storageService, customer, bucket);
			}

			bucketMetaData = bucketMetaDataCache.putIfAbsent(key, newBucketMetaData);
			if (bucketMetaData == null)
			{
				bucketMetaData = newBucketMetaData;
			}
		}

		return bucketMetaData;
	}
}
