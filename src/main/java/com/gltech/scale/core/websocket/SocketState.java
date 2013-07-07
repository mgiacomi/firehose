package com.gltech.scale.core.websocket;

import org.eclipse.jetty.websocket.api.Session;

public interface SocketState
{
	void onConnect(Session session);

	void onClose(Session session, int statusCode, String reason);

	void onError(Session session, Throwable error);
}
