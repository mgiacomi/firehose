package com.gltech.scale.core.collector;

import com.gltech.scale.core.event.EventPayload;

public interface EventStream
{
	EventPayload getCurrentEventPayload();

	void nextRecord();

	public void close();
}
