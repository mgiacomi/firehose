package com.gltech.scale.core.storage.bytearray;

import com.gltech.scale.core.storage.BucketMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TermVoldemortStorage implements InternalStorage
{
	private static final Logger logger = LoggerFactory.getLogger("com.lokiscale.storage.VoldemortStorage");
	private final VoldemortStorage shortTermPayloadStorage;
	private final VoldemortStorage mediumTermPayloadStorage;
	private final VoldemortStorage largeTermPayloadStorage;

	public TermVoldemortStorage()
	{
		shortTermPayloadStorage = new VoldemortStorage("ShortTermPayloadStorage");
		mediumTermPayloadStorage = new VoldemortStorage("MediumTermPayloadStorage");
		largeTermPayloadStorage = new VoldemortStorage("LargeTermPayloadStorage");
	}

	public void putBucket(final BucketMetaData bucketMetaData)
	{
		shortTermPayloadStorage.putBucket(bucketMetaData);
	}

	public BucketMetaData getBucket(String customer, String bucket)
	{
		return shortTermPayloadStorage.getBucket(customer, bucket);
	}

	public StoragePayload internalGetPayload(BucketMetaData bucketMetaData, String id)
	{
		return getVoldemortStorage(bucketMetaData.getLifeTime()).internalGetPayload(bucketMetaData, id);
	}

	public StoragePayload getPayload(String customer, String bucket, String id)
	{
		return internalGetPayload(getBucket(customer, bucket), id);
	}

	public void internalPutPayload(BucketMetaData bucketMetaData, StoragePayload storagePayload)
	{
		getVoldemortStorage(bucketMetaData.getLifeTime()).internalPutPayload(bucketMetaData, storagePayload);
	}

	public void putPayload(StoragePayload storagePayload)
	{
		internalPutPayload(getBucket(storagePayload.getCustomer(), storagePayload.getBucket()), storagePayload);
	}

	private VoldemortStorage getVoldemortStorage(BucketMetaData.LifeTime lifeTime)
	{
		if (lifeTime.equals(BucketMetaData.LifeTime.large))
		{
			return largeTermPayloadStorage;
		}
		else if (lifeTime.equals(BucketMetaData.LifeTime.medium))
		{
			return mediumTermPayloadStorage;
		}
		else
		{
			return shortTermPayloadStorage;
		}
	}
}
