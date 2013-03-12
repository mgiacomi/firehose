package com.gltech.scale.core.storage.bytearray;

import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.storage.DuplicateBucketException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class MemoryStorage implements InternalStorage
{
	private static final Logger logger = LoggerFactory.getLogger(MemoryStorage.class);

	private ConcurrentHashMap<String, ChannelMetaData> bucketMap = new ConcurrentHashMap<>();

	private ConcurrentHashMap<ChannelMetaData, ConcurrentHashMap<String, StoragePayload>> payloadsMap =
			new ConcurrentHashMap<ChannelMetaData, ConcurrentHashMap<String, StoragePayload>>();

	/**
	 * @param channel
	 * @throws com.gltech.scale.core.storage.StorageException
	 *          is bucket already exists
	 */
	@Override
	public void putBucket(ChannelMetaData channel)
	{
		String key = createBucketKey(channel);
		logger.debug("creating {}", channel);
		if (bucketMap.containsKey(key))
		{
			throw new DuplicateBucketException("Bucket already exists " + channel.getCustomer() + " " + channel.getBucket());
		}
		bucketMap.put(key, channel);
		payloadsMap.putIfAbsent(channel, new ConcurrentHashMap<String, StoragePayload>());
	}

	private String createBucketKey(ChannelMetaData channel)
	{
		return createBucketKey(channel.getCustomer(), channel.getBucket());
	}

	private String createBucketKey(String customer, String bucket)
	{
		return customer + "|" + bucket;
	}

	@Override
	public ChannelMetaData getBucket(String customer, String bucket)
	{
		return bucketMap.get(createBucketKey(customer, bucket));
	}

	@Override
	public void putPayload(StoragePayload storagePayload)
	{
		logger.debug("putting payload {}", storagePayload);
		ChannelMetaData channelMetaData = getBucket(storagePayload.getCustomer(), storagePayload.getBucket());
		internalPutPayload(channelMetaData, storagePayload);
	}

	@Override
	public StoragePayload getPayload(String customer, String bucket, String id)
	{
		ChannelMetaData channelMetaData = getBucket(customer, bucket);
		StoragePayload payload = internalGetPayload(channelMetaData, id);
		if (payload != null)
		{
			payload.setChannelMetaData(channelMetaData);
		}
		return payload;
	}

	public StoragePayload internalGetPayload(ChannelMetaData channelMetaData, String id)
	{
		return payloadsMap.get(channelMetaData).get(id);
	}

	public void internalPutPayload(ChannelMetaData channelMetaData, StoragePayload storagePayload)
	{
		payloadsMap.get(channelMetaData).put(storagePayload.getId(), storagePayload);
	}
}
