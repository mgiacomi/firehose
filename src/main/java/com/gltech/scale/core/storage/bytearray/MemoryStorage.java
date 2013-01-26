package com.gltech.scale.core.storage.bytearray;

import com.gltech.scale.core.storage.BucketMetaData;
import com.gltech.scale.core.storage.DuplicateBucketException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class MemoryStorage implements InternalStorage
{
	private static final Logger logger = LoggerFactory.getLogger("com.lokiscale.storage.MemoryStorage");

	private ConcurrentHashMap<String, BucketMetaData> bucketMap = new ConcurrentHashMap<>();

	private ConcurrentHashMap<BucketMetaData, ConcurrentHashMap<String, StoragePayload>> payloadsMap =
			new ConcurrentHashMap<BucketMetaData, ConcurrentHashMap<String, StoragePayload>>();

	/**
	 * @param bucket
	 * @throws com.gltech.scale.core.storage.StorageException
	 *          is bucket already exists
	 */
	@Override
	public void putBucket(BucketMetaData bucket)
	{
		String key = createBucketKey(bucket);
		logger.debug("creating {}", bucket);
		if (bucketMap.containsKey(key))
		{
			throw new DuplicateBucketException("Bucket already exists " + bucket.getCustomer() + " " + bucket.getBucket());
		}
		bucketMap.put(key, bucket);
		payloadsMap.putIfAbsent(bucket, new ConcurrentHashMap<String, StoragePayload>());
	}

	private String createBucketKey(BucketMetaData bucket)
	{
		return createBucketKey(bucket.getCustomer(), bucket.getBucket());
	}

	private String createBucketKey(String customer, String bucket)
	{
		return customer + "|" + bucket;
	}

	@Override
	public BucketMetaData getBucket(String customer, String bucket)
	{
		return bucketMap.get(createBucketKey(customer, bucket));
	}

	@Override
	public void putPayload(StoragePayload storagePayload)
	{
		logger.debug("putting payload {}", storagePayload);
		BucketMetaData bucketMetaData = getBucket(storagePayload.getCustomer(), storagePayload.getBucket());
		internalPutPayload(bucketMetaData, storagePayload);
	}

	@Override
	public StoragePayload getPayload(String customer, String bucket, String id)
	{
		BucketMetaData bucketMetaData = getBucket(customer, bucket);
		StoragePayload payload = internalGetPayload(bucketMetaData, id);
		if (payload != null)
		{
			payload.setBucketMetaData(bucketMetaData);
		}
		return payload;
	}

	public StoragePayload internalGetPayload(BucketMetaData bucketMetaData, String id)
	{
		return payloadsMap.get(bucketMetaData).get(id);
	}

	public void internalPutPayload(BucketMetaData bucketMetaData, StoragePayload storagePayload)
	{
		payloadsMap.get(bucketMetaData).put(storagePayload.getId(), storagePayload);
	}
}
