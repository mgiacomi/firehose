package com.gltech.scale.core.storage;

import com.gltech.scale.core.model.ChannelMetaData;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public interface Storage
{
	void putChannelMetaData(ChannelMetaData channelMetaData);

	ChannelMetaData getChannelMetaData(String channelName);

	void putBytes(ChannelMetaData channelMetaData, String id, byte[] data);

	byte[] getBytes(ChannelMetaData channelMetaData, String id);

	void putMessages(ChannelMetaData channelMetaData, String id, InputStream inputStream);

	void getMessages(ChannelMetaData channelMetaData, String id, OutputStream outputStream);
}
