package com.gltech.scale.core.storage.bytearray;

import com.gltech.scale.core.model.ChannelMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BucketOnlyStorage implements InternalStorage
{
	private static final Logger logger = LoggerFactory.getLogger(BucketOnlyStorage.class);
	InternalStorage memoryStorage = new MemoryStorage();
	private static final long MegaBytes = 1024L * 1024L;

	public void putBucket(ChannelMetaData channelMetaData)
	{
		memoryStorage.putBucket(channelMetaData);
	}

	public ChannelMetaData getBucket(String customer, String bucket)
	{
		return memoryStorage.getBucket(customer, bucket);
	}

	public void putPayload(StoragePayload storagePayload)
	{
		try
		{
			logger.info("I didn't write " + storagePayload.getPayload().length / MegaBytes + "m worth of data.");
		}
		catch (Exception e)
		{
			// Maybe the math could fail?
		}
	}

	public StoragePayload getPayload(String customer, String bucket, String id)
	{
		return null;
	}

	public StoragePayload internalGetPayload(ChannelMetaData channelMetaData, String id)
	{
		//do nothing
		return null;
	}

	public void internalPutPayload(ChannelMetaData channelMetaData, StoragePayload storagePayload)
	{
		try
		{
			logger.info("I didn't write " + storagePayload.getPayload().length / MegaBytes + "m worth of data.");
		}
		catch (Exception e)
		{
			// Maybe the math could fail?
		}
	}
}
