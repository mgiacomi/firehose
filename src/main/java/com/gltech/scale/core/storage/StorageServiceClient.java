package com.gltech.scale.core.storage;

import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.model.ChannelMetaData;

import java.io.InputStream;

public interface StorageServiceClient
{
	ChannelMetaData getChannelMetaData(ServiceMetaData storageService, String name);

	void putBucketMetaData(ServiceMetaData storageService, ChannelMetaData channelMetaData);

	InputStream getEventStream(ServiceMetaData storageService, String channelName, String id);

	byte[] get(ServiceMetaData storageService, String channelName, String id);

	void put(ServiceMetaData storageService, String channelName, String id, InputStream inputStream);

	void put(ServiceMetaData storageService, String channelName, String id, byte[] payload);
}
