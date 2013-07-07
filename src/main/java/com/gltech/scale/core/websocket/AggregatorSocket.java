package com.gltech.scale.core.websocket;

import com.gltech.scale.core.model.Message;
import com.gltech.scale.core.model.ModelIO;
import com.google.common.base.Throwables;
import com.google.common.collect.MapMaker;
import com.google.inject.Inject;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;

public class AggregatorSocket
{
	private static final Logger logger = LoggerFactory.getLogger(AggregatorSocket.class);
	private ConcurrentMap<Integer, ResponseCallback> callbackMap = new MapMaker().concurrencyLevel(16).weakValues().makeMap();
	private SocketState socketState;
	private Session session;
	private SocketIO socketIO;

	@Inject
	public AggregatorSocket(SocketState socketState, SocketIO socketIO)
	{
		this.socketState = socketState;
		this.socketIO = socketIO;
	}

	public void send(int id, byte[] message, ResponseCallback callback)
	{
		try
		{
			SocketRequest request = new SocketRequest(id, message);

			// Register our callback for returning response
			callbackMap.put(id, callback);

			session.getRemote().sendBytesByFuture(ByteBuffer.wrap(socketIO.toBytes(request)));
		}
		catch (Exception e)
		{
			throw Throwables.propagate(e);
		}
	}

	@OnWebSocketMessage
	public void onMessage(InputStream inputStream)
	{
		SocketResponse response = new SocketResponse(inputStream);

		try
		{
			// Get the callback and add our response to it.
			callbackMap.get(response.getId()).add(response);
		}
		catch (NullPointerException e)
		{
			// If we get an NPE it just means the reference has been garbage collected which is possible and fine.
		}
	}

	@OnWebSocketConnect
	public void onConnect(Session session)
	{
		this.session = session;
		logger.info("Connected to {}", session.getRemoteAddress());
		socketState.onConnect(session);
	}

	@OnWebSocketClose
	public void onClose(Session session, int statusCode, String reason)
	{
		logger.info("Session closed statusCode={}, reason=\"{}\" host={}", statusCode, reason, session.getRemoteAddress());
		socketState.onClose(session, statusCode, reason);
	}

	@OnWebSocketError
	public void onError(Session session, Throwable error)
	{
		logger.info("WebSocket Error for host {}", session.getRemoteAddress(), error);
		socketState.onError(session, error);
	}
}
