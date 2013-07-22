package com.gltech.scale.core.aggregator.clientserver;

import com.gltech.scale.core.aggregator.Aggregator;
import com.gltech.scale.core.websocket.SocketRequest;
import com.gltech.scale.core.websocket.SocketResponse;
import com.google.inject.Inject;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

@WebSocket
public class AggregatorServerSocket
{
	private static final Logger logger = LoggerFactory.getLogger(AggregatorServerSocket.class);
	private final Set<Session> sessions = new CopyOnWriteArraySet<>();
	private Aggregator aggregator;

	@Inject
	public AggregatorServerSocket(Aggregator aggregator)
	{
		this.aggregator = aggregator;
	}

	@OnWebSocketConnect
	public void onConnect(Session session)
	{
		logger.info("Adding session {}", session);
		sessions.add(session);
	}

	@OnWebSocketMessage
	public void onMessage(Session session, byte buffer[], int offset, int length)
	{
		byte[] requestData = new byte[length];
		System.arraycopy(buffer, offset, requestData, 0, length);

		SocketRequest request = new SocketRequest(requestData);

		try
		{
			if (request.isPrimary())
			{
				aggregator.addMessage(request.getChannelName(), request.getData(), request.getPeriod());
			}
			else {
				aggregator.addBackupMessage(request.getChannelName(), request.getData(), request.getPeriod());
			}

			ByteBuffer response = new SocketResponse(request.getId(), SocketResponse.ACK).getByteBuffer();
			session.getRemote().sendBytes(response);
		}
		catch (Exception e)
		{
			SocketResponse socketResponse = new SocketResponse(request.getId(), SocketResponse.ERROR, e.getMessage().getBytes());

			try
			{
				session.getRemote().sendBytes(socketResponse.getByteBuffer());
			}
			catch (IOException ioe)
			{
				logger.error("Failed to return error back over WebSocket {}", session.getRemoteAddress(), ioe);
			}
		}
	}

	@OnWebSocketClose
	public void onClose(Session session, int statusCode, String reason)
	{
		logger.info("Removing session statusCode={}, reason={} session={}", statusCode, reason, session);
		sessions.remove(session);
	}

	@OnWebSocketError
	public void onError(Session session, Throwable error)
	{
		logger.info("WebSocket Error for session {}", session, error);
		sessions.remove(session);
	}
}
