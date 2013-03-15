package com.gltech.scale.core.storage.bytearray;

import com.gltech.scale.core.model.ChannelMetaData;

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

	public void putBucket(ChannelMetaData channelMetaData)
	{
		delegate.putBucket(channelMetaData);
	}

	public ChannelMetaData getBucket(String channelName)
	{
		return delegate.getBucket(channelName);
	}

	public void putPayload(StoragePayload storagePayload)
	{
		ChannelMetaData channelMetaData = getBucket(storagePayload.getChannelMetaData().getName());

		List<String> previousVersions = storagePayload.getPreviousVersions();

		if (previousVersions.isEmpty())
		{
			delegate.internalPutPayload(channelMetaData, storagePayload);
			return;
		}
		StoragePayload payload = delegate.internalGetPayload(channelMetaData, storagePayload.getId());
		if (null == payload)
		{
			throw new InvalidVersionException("payload does not yet exist");
		}
		String currentVersion = payload.getVersion();
		for (String previousVersion : previousVersions)
		{
			if (currentVersion.equals(previousVersion) || previousVersion.equals("*"))
			{
				delegate.internalPutPayload(channelMetaData, storagePayload);
				return;
			}
		}
		throw new InvalidVersionException("unable to match existing version of payload");
	}

	public StoragePayload getPayload(String channelName, String id)
	{
		return delegate.getPayload(channelName, id);
	}


}
