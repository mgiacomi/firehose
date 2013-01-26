package com.gltech.scale.core.storage.bytearray;

import com.gltech.scale.core.storage.BucketMetaData;

public interface ByteArrayStorage
{
	void putBucket(BucketMetaData bucketMetaData);

	BucketMetaData getBucket(String customer, String bucket);

	void putPayload(StoragePayload storagePayload);

	StoragePayload getPayload(String customer, String bucket, String id);

	//todo - gfm - 9/24/12 - implement delete
}
