package com.gltech.scale.core.storage.bytearray;

import com.gltech.scale.core.storage.BucketMetaData;

/**
 * This interface exposes some convenience methods to save on (potentially) remote calls.
 */
public interface InternalStorage extends ByteArrayStorage
{
	StoragePayload internalGetPayload(BucketMetaData bucketMetaData, String id);

	void internalPutPayload(BucketMetaData bucketMetaData, StoragePayload storagePayload);
}
