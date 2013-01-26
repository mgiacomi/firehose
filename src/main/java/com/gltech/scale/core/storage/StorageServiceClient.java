package com.gltech.scale.core.storage;

import com.gltech.scale.core.coordination.registration.ServiceMetaData;

import java.io.InputStream;

public interface StorageServiceClient
{
	BucketMetaData getBucketMetaData(ServiceMetaData storageService, String customer, String bucket);

	void putBucketMetaData(ServiceMetaData storageService, BucketMetaData bucketMetaData);

	InputStream getEventStream(ServiceMetaData storageService, String customer, String bucket, String id);

	byte[] get(ServiceMetaData storageService, String customer, String bucket, String id);

	void put(ServiceMetaData storageService, String customer, String bucket, String id, InputStream inputStream);

	void put(ServiceMetaData storageService, String customer, String bucket, String id, byte[] payload);
}
