package com.gltech.scale.core.storage;

import com.gltech.scale.core.model.ChannelMetaData;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public interface Storage
{
	void put(ChannelMetaData channelMetaData);

	ChannelMetaData get(String channelName);

	void putMessages(String channelName, String id, InputStream inputStream, Map<String, List<String>> headers);

	void getMessages(String channelName, String id, OutputStream outputStream);
}
