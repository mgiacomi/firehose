package com.gltech.scale.core.storage;

import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.model.ChannelMetaData;

import java.io.InputStream;

public interface StorageServiceClient
{
	ChannelMetaData getBucketMetaData(ServiceMetaData storageService, String customer, String bucket);

	void putBucketMetaData(ServiceMetaData storageService, ChannelMetaData channelMetaData);

	InputStream getEventStream(ServiceMetaData storageService, String customer, String bucket, String id);

	byte[] get(ServiceMetaData storageService, String customer, String bucket, String id);

	void put(ServiceMetaData storageService, String customer, String bucket, String id, InputStream inputStream);

	void put(ServiceMetaData storageService, String customer, String bucket, String id, byte[] payload);
}
