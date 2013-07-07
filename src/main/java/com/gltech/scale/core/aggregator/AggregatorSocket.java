package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.websocket.SocketIO;
import com.gltech.scale.core.websocket.SocketRequest;
import com.gltech.scale.core.websocket.SocketResponse;
import com.google.inject.Inject;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.*;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

@WebSocket
public class AggregatorSocket
{
	private static final Logger logger = LoggerFactory.getLogger(AggregatorSocket.class);
	private final Set<Session> sessions = new CopyOnWriteArraySet<>();
	private Aggregator aggregator;
	private SocketIO socketIO;

	@Inject
	public AggregatorSocket(Aggregator aggregator, SocketIO socketIO)
	{
		this.aggregator = aggregator;
		this.socketIO = socketIO;
	}

	@OnWebSocketConnect
	public void onConnect(Session session)
	{
		logger.info("Adding session {}", session);
		sessions.add(session);
	}

	@OnWebSocketMessage
	public void onMessage(Session session, InputStream inputStream)
	{
		SocketRequest socketRequest = socketIO.toSocketRequest(inputStream);
		DateTime period = new DateTime(socketRequest.getHeader("nearestPeriodCeiling"));
		String channelName = socketRequest.getHeader("channelName");
		byte[] data = socketRequest.getData();
		String mode = socketRequest.getHeader("mode");

		try
		{
			if("primary".equalsIgnoreCase(mode))
			{
				aggregator.addMessage(channelName, data, period);
			}
			if("backup".equalsIgnoreCase(mode))
			{
				aggregator.addBackupMessage(channelName, data, period);
			}

			session.getRemote().sendBytesByFuture(new SocketResponse(0, SocketResponse.ACK).getByteBuffer());
		}
		catch (Exception e)
		{
			SocketResponse socketResponse = new SocketResponse(0, SocketResponse.ERROR, e.getMessage().getBytes());
			session.getRemote().sendBytesByFuture(socketResponse.getByteBuffer());
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
