package com.gltech.scale.core.storage.bytearray;

import com.gltech.scale.core.model.ChannelMetaData;

public interface ByteArrayStorage
{
	void putBucket(ChannelMetaData channelMetaData);

	ChannelMetaData getBucket(String customer, String bucket);

	void putPayload(StoragePayload storagePayload);

	StoragePayload getPayload(String customer, String bucket, String id);

	//todo - gfm - 9/24/12 - implement delete
}
