package com.gltech.scale.core.storage.bytearray;

import com.gltech.scale.core.model.ChannelMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TermVoldemortStorage implements InternalStorage
{
	private static final Logger logger = LoggerFactory.getLogger(TermVoldemortStorage.class);
	private final VoldemortStorage shortTermPayloadStorage;
	private final VoldemortStorage mediumTermPayloadStorage;
	private final VoldemortStorage largeTermPayloadStorage;

	public TermVoldemortStorage()
	{
		shortTermPayloadStorage = new VoldemortStorage("ShortTermPayloadStorage");
		mediumTermPayloadStorage = new VoldemortStorage("MediumTermPayloadStorage");
		largeTermPayloadStorage = new VoldemortStorage("LargeTermPayloadStorage");
	}

	public void putBucket(final ChannelMetaData channelMetaData)
	{
		shortTermPayloadStorage.putBucket(channelMetaData);
	}

	public ChannelMetaData getBucket(String channelName)
	{
//		return shortTermPayloadStorage.getBucket(channelName);
		return null;
	}

	public StoragePayload internalGetPayload(ChannelMetaData channelMetaData, String id)
	{
//		return getVoldemortStorage(channelMetaData.getLifeTime()).internalGetPayload(channelMetaData, id);
		return null;
	}

	public StoragePayload getPayload(String channelName, String id)
	{
		return internalGetPayload(getBucket(channelName), id);
	}

	public void internalPutPayload(ChannelMetaData channelMetaData, StoragePayload storagePayload)
	{
		//getVoldemortStorage(channelMetaData.getLifeTime()).internalPutPayload(channelMetaData, storagePayload);
	}

	public void putPayload(StoragePayload storagePayload)
	{
		internalPutPayload(getBucket(storagePayload.getChannelMetaData().getName()), storagePayload);
	}
/*
	private VoldemortStorage getVoldemortStorage(ChannelMetaData.LifeTime lifeTime)
	{
		if (lifeTime.equals(ChannelMetaData.LifeTime.large))
		{
			return largeTermPayloadStorage;
		}
		else if (lifeTime.equals(ChannelMetaData.LifeTime.medium))
		{
			return mediumTermPayloadStorage;
		}
		else
		{
			return shortTermPayloadStorage;
		}
	}
*/
}
