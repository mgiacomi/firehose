package com.gltech.scale.core.storage;

import com.gltech.scale.core.model.ChannelMetaData;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public interface Storage
{
	void putBucket(ChannelMetaData channelMetaData);

	ChannelMetaData getBucket(String customer, String bucket);

	void putPayload(String customer, String bucket, String id, InputStream inputStream, Map<String, List<String>> headers);

	void getPayload(String customer, String bucket, String id, OutputStream outputStream);

	//todo - gfm - 9/24/12 - implement delete
}
