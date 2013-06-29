package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.model.BatchMetaData;
import com.gltech.scale.core.model.ChannelMetaData;
import org.joda.time.DateTime;

import java.io.OutputStream;

public interface Batch
{
	void addMessage(byte[] message);

	ChannelMetaData getChannelMetaData();

	long getMessages();

	long getBytes();

	long writeMessages(OutputStream outputStream);

	DateTime getLastMessageTime();

	DateTime getFirstMessageTime();

	BatchMetaData getMetaData();

	void clear();
}
