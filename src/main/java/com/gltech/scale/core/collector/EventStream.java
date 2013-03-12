package com.gltech.scale.core.collector;

import com.gltech.scale.core.model.Message;

public interface EventStream
{
	Message getCurrentMessage();

	void nextRecord();

	public void close();
}
