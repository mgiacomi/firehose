package com.gltech.scale.core.websocket;

import org.eclipse.jetty.websocket.api.Session;

import java.util.UUID;

public interface SocketState
{
	void onConnect(UUID workerId, Session session);

	void onClose(UUID workerId, Session session, int statusCode, String reason);

	void onError(UUID workerId, Session session, Throwable error);
}
