package com.gltech.scale.core.aggregator.clientserver;

import static org.mockito.Mockito.*;

import static org.junit.Assert.*;

import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.websocket.ResponseCallback;
import com.gltech.scale.core.websocket.SocketResponse;
import com.gltech.scale.core.websocket.SocketState;
import org.eclipse.jetty.websocket.api.*;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.Future;

public class AggregatorClientSocketTest
{
	@Test
	public void testSend() throws Exception
	{
		ServiceMetaData serviceMetaData = mock(ServiceMetaData.class);
		when(serviceMetaData.getWorkerId()).thenReturn(UUID.randomUUID());

		SocketState socketState = mock(SocketState.class);
		AggregatorClientSocket clientSocket = new AggregatorClientSocket(serviceMetaData.getWorkerId(), socketState);
		clientSocket.onConnect(getSession());

		ResponseCallback callback = new ResponseCallback(serviceMetaData, null);
		clientSocket.sendPrimary(1234, "channel", DateTime.now(), "test".getBytes(), callback);

		assertFalse(callback.hasResponse());

		SocketResponse response = new SocketResponse(1234, SocketResponse.ACK);
		clientSocket.onMessage(response.getByteBuffer().array(), 0, response.getByteBuffer().array().length);

		assertTrue(callback.hasResponse());
		assertTrue(response.isAck());
	}

	@Test
	public void testSocketState() throws Exception
	{
		ServiceMetaData serviceMetaData = mock(ServiceMetaData.class);
		Session session = getSession();
		SocketState socketState = mock(SocketState.class);
		AggregatorClientSocket clientSocket = new AggregatorClientSocket(serviceMetaData.getWorkerId(), socketState);

		clientSocket.onConnect(session);
		verify(socketState).onConnect(serviceMetaData.getWorkerId(), session);

		String reason = "becase I'm testing it";
		clientSocket.onClose(session, 123, reason);
		verify(socketState).onClose(serviceMetaData.getWorkerId(), session, 123, reason);

		IllegalStateException error = new IllegalStateException("I love creating errors");
		clientSocket.onError(session, error);
		verify(socketState).onError(serviceMetaData.getWorkerId(), session, error);
	}

	private Session getSession()
	{
		return new Session()
		{
			public void close() throws IOException
			{
			}

			public void close(CloseStatus closeStatus) throws IOException
			{
			}

			public void close(int statusCode, String reason) throws IOException
			{
			}

			public void disconnect() throws IOException
			{
			}

			public long getIdleTimeout()
			{
				return 0;
			}

			public InetSocketAddress getLocalAddress()
			{
				return new InetSocketAddress("local.gltech.com", 8080);
			}

			public long getMaximumMessageSize()
			{
				return 0;
			}

			public WebSocketPolicy getPolicy()
			{
				return null;
			}

			public String getProtocolVersion()
			{
				return null;
			}

			public RemoteEndpoint getRemote()
			{
				return new RemoteEndpoint()
				{
					public void sendBytes(ByteBuffer data) throws IOException
					{
					}

					public Future<Void> sendBytesByFuture(ByteBuffer data)
					{
						return null;
					}

					public void sendPartialBytes(ByteBuffer fragment, boolean isLast) throws IOException
					{
					}

					public void sendPartialString(String fragment, boolean isLast) throws IOException
					{
					}

					public void sendPing(ByteBuffer applicationData) throws IOException
					{
					}

					public void sendPong(ByteBuffer applicationData) throws IOException
					{
					}

					public void sendString(String text) throws IOException
					{
					}

					public Future<Void> sendStringByFuture(String text)
					{
						return null;
					}
				};
			}

			public InetSocketAddress getRemoteAddress()
			{
				return new InetSocketAddress("remote.gltech.com", 8080);
			}

			public UpgradeRequest getUpgradeRequest()
			{
				return null;
			}

			public UpgradeResponse getUpgradeResponse()
			{
				return null;
			}

			public boolean isOpen()
			{
				return false;
			}

			public boolean isSecure()
			{
				return false;
			}

			public void setIdleTimeout(long ms)
			{
			}

			public void setMaximumMessageSize(long length)
			{
			}

			public SuspendToken suspend()
			{
				return null;
			}
		};
	}
}
