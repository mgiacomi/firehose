package com.gltech.scale.core.storage;

import com.google.inject.Inject;
import com.gltech.scale.core.coordination.CoordinationService;
import com.gltech.scale.core.coordination.registration.ServiceMetaData;
import com.gltech.scale.core.util.Http404Exception;

import javax.ws.rs.core.MediaType;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class BucketMetaDataCache
{
	static private ConcurrentMap<String, BucketMetaData> bucketMetaDataCache = new ConcurrentHashMap<>();
	private StorageServiceClient storageServiceClient;
	private CoordinationService coordinationService;

	@Inject
	public BucketMetaDataCache(CoordinationService coordinationService, StorageServiceClient storageServiceClient)
	{
		this.storageServiceClient = storageServiceClient;
		this.coordinationService = coordinationService;
	}

	public BucketMetaData getBucketMetaData(String customer, String bucket, boolean createIfNotExist)
	{
		String key = "/" + customer + "/" + bucket;

		BucketMetaData bucketMetaData = bucketMetaDataCache.get(key);

		if (bucketMetaData == null)
		{
			ServiceMetaData storageService = coordinationService.getRegistrationService().getStorageServiceRoundRobin();
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
