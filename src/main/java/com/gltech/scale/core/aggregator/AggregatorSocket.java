package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.websocket.SocketIO;
import com.gltech.scale.core.websocket.SocketRequest;
import com.gltech.scale.core.websocket.SocketResponse;
import com.google.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.*;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
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
	public void onMessage(Session session, byte buffer[], int offset, int length)
	{
		byte[] requestData = new byte[length];
		System.arraycopy(buffer, offset, requestData, 0, length);

		SocketRequest socketRequest = socketIO.toSocketRequest(requestData);
		DateTime period = DateTimeFormat.forPattern("yyyyMMddHHmmss").parseDateTime(socketRequest.getHeader("nearestPeriodCeiling"));
		String channelName = socketRequest.getHeader("channelName");
		String mode = socketRequest.getHeader("mode");
		byte[] data = socketRequest.getData();

		try
		{
			if ("primary".equalsIgnoreCase(mode))
			{
				aggregator.addMessage(channelName, data, period);
			}
			else if ("backup".equalsIgnoreCase(mode))
			{
				aggregator.addBackupMessage(channelName, data, period);
			}
			else {
				throw new RuntimeException("Mode must be either set to 'primary' or 'backup'.  Message was ignored.");
			}

			ByteBuffer response = new SocketResponse(socketRequest.getId(), SocketResponse.ACK).getByteBuffer();
			session.getRemote().sendBytes(response);
		}
		catch (Exception e)
		{
			SocketResponse socketResponse = new SocketResponse(socketRequest.getId(), SocketResponse.ERROR, e.getMessage().getBytes());

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
