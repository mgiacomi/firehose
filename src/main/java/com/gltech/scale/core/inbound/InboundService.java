package com.gltech.scale.core.inbound;

import com.gltech.scale.lifecycle.LifeCycle;
import com.gltech.scale.core.model.ChannelMetaData;
import org.joda.time.DateTime;

import javax.ws.rs.core.MediaType;
import java.io.OutputStream;

public interface InboundService extends LifeCycle
{
	void addMessage(String channelName, MediaType mediaTypes, byte[] payload);

	int writeMessagesToOutputStream(String channelName, DateTime dateTime, OutputStream outputStream, int recordsWritten);
}
