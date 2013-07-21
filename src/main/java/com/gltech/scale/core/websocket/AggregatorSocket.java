package com.gltech.scale.core.websocket;

import com.google.common.base.Throwables;
import com.google.common.collect.MapMaker;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.*;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

@WebSocket
public class AggregatorSocket
{
	private static final Logger logger = LoggerFactory.getLogger(AggregatorSocket.class);
	private ConcurrentMap<Integer, ResponseCallback> callbackMap = new MapMaker().concurrencyLevel(16).weakValues().makeMap();
	private SocketState socketState;
	private UUID workerId;
	private Session session;

	public AggregatorSocket(UUID workerId, SocketState socketState)
	{
		this.workerId = workerId;
		this.socketState = socketState;
	}

	public void sendPrimary(int id, String channelName, DateTime nearestPeriodCeiling, byte[] message, ResponseCallback callback)
	{
		send(id, channelName, nearestPeriodCeiling, "primary", message, callback);
	}

	public void sendBackup(int id, String channelName, DateTime nearestPeriodCeiling, byte[] message, ResponseCallback callback)
	{
		send(id, channelName, nearestPeriodCeiling, "backup", message, callback);
	}

	private void send(int id, String channelName, DateTime nearestPeriodCeiling, String mode, byte[] message, ResponseCallback callback)
	{
		int mode_i = "primary".equalsIgnoreCase(mode) ? 0 : 1;

		try
		{
			SocketRequest request = new SocketRequest(id, mode_i, channelName, nearestPeriodCeiling, message);

			// Register our callback for returning response
			callbackMap.put(id, callback);

			Future<Void> future = session.getRemote().sendBytesByFuture(request.getByteBuffer());
			callback.setRequestFuture(workerId, future);
		}
		catch (Exception e)
		{
			throw Throwables.propagate(e);
		}
	}

	@OnWebSocketMessage
	public void onMessage(byte buffer[], int offset, int length)
	{
		try
		{
			byte[] responseData = new byte[length];
			System.arraycopy(buffer, offset, responseData, 0, length);

			SocketResponse response = new SocketResponse(responseData);

			try
			{
				callbackMap.get(response.getId()).setResponse(workerId, response);
			}
			catch (NullPointerException e)
			{
				// If we get an NPE it just means the reference has been garbage collected which is fine.
			}
		}
		catch (Exception e)
		{
			logger.error("Failed to read SocketResponse.", e);
		}
	}

	@OnWebSocketConnect
	public void onConnect(Session session)
	{
		this.session = session;
		logger.info("Connected to {}", session.getRemoteAddress());
		socketState.onConnect(workerId, session);
	}

	@OnWebSocketClose
	public void onClose(Session session, int statusCode, String reason)
	{
		logger.info("Session closed statusCode={}, reason=\"{}\" host={}", statusCode, reason, session.getRemoteAddress());
		socketState.onClose(workerId, session, statusCode, reason);
	}

	@OnWebSocketError
	public void onError(Session session, Throwable error)
	{
		logger.info("WebSocket Error for host {}", session.getRemoteAddress(), error);
		socketState.onError(workerId, session, error);
	}
}
