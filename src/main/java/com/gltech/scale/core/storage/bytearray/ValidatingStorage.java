package com.gltech.scale.core.storage.bytearray;

import com.gltech.scale.core.storage.BucketMetaData;

import java.util.List;

/**
 * Performs common validation before delegating to InternalStorage.
 */
public class ValidatingStorage implements ByteArrayStorage
{
	private InternalStorage delegate;

	public ValidatingStorage(InternalStorage delegate)
	{
		this.delegate = delegate;
	}

	public void putBucket(BucketMetaData bucketMetaData)
	{
		delegate.putBucket(bucketMetaData);
	}

	public BucketMetaData getBucket(String customer, String bucket)
	{
		return delegate.getBucket(customer, bucket);
	}

	public void putPayload(StoragePayload storagePayload)
	{
		BucketMetaData bucketMetaData = getBucket(storagePayload.getCustomer(), storagePayload.getBucket());

		List<String> previousVersions = storagePayload.getPreviousVersions();

		if (previousVersions.isEmpty())
		{
			delegate.internalPutPayload(bucketMetaData, storagePayload);
			return;
		}
		StoragePayload payload = delegate.internalGetPayload(bucketMetaData, storagePayload.getId());
		if (null == payload)
		{
			throw new InvalidVersionException("payload does not yet exist");
		}
		String currentVersion = payload.getVersion();
		for (String previousVersion : previousVersions)
		{
			if (currentVersion.equals(previousVersion) || previousVersion.equals("*"))
			{
				delegate.internalPutPayload(bucketMetaData, storagePayload);
				return;
			}
		}
		throw new InvalidVersionException("unable to match existing version of payload");
	}

	public StoragePayload getPayload(String customer, String bucket, String id)
	{
		return delegate.getPayload(customer, bucket, id);
	}


}
