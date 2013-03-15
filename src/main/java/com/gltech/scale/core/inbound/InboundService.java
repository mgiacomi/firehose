package com.gltech.scale.core.inbound;

import com.gltech.scale.lifecycle.LifeCycle;
import com.gltech.scale.core.model.ChannelMetaData;
import org.joda.time.DateTime;

import java.io.OutputStream;

public interface InboundService extends LifeCycle
{
	void addEvent(String channelName, byte[] payload);

	int writeEventsToOutputStream(ChannelMetaData channelMetaData, DateTime dateTime, OutputStream outputStream, int recordsWritten);
}
