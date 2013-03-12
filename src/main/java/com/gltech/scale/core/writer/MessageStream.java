package com.gltech.scale.core.writer;

import com.gltech.scale.core.model.Message;

public interface MessageStream
{
	Message getCurrentMessage();

	void nextRecord();

	public void close();
}
