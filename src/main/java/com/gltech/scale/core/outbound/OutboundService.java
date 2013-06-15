package com.gltech.scale.core.outbound;

import com.gltech.scale.lifecycle.LifeCycle;

import javax.ws.rs.core.StreamingOutput;

public interface OutboundService extends LifeCycle
{
	StreamingOutput getMessages(final String channelName, final int year, final int month, final int day, final int hour, final int min, final int sec);
}
