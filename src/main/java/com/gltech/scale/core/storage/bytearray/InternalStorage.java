package com.gltech.scale.core.storage.bytearray;

import com.gltech.scale.core.model.ChannelMetaData;

/**
 * This interface exposes some convenience methods to save on (potentially) remote calls.
 */
public interface InternalStorage extends ByteArrayStorage
{
	StoragePayload internalGetPayload(ChannelMetaData channelMetaData, String id);

	void internalPutPayload(ChannelMetaData channelMetaData, StoragePayload storagePayload);
}
