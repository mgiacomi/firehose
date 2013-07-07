package com.gltech.scale.core.websocket;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class ResponseCallback
{
	private volatile List<SocketResponse> responses = new CopyOnWriteArrayList<>();

	public void add(SocketResponse response)
	{
		responses.add(response);
	}

	public boolean hasResponse() {
		return responses.size() > 0;
	}

	public List<SocketResponse> getResponses()
	{
		return responses;
	}
}
