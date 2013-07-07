package com.gltech.scale.core.websocket;

import static org.mockito.Mockito.*;

import static org.junit.Assert.*;
import org.eclipse.jetty.websocket.api.*;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;

public class AggregatorSocketTest
{
	@Test
	public void testSend() throws Exception
	{
		SocketState socketState = mock(SocketState.class);
		AggregatorSocket socket = new AggregatorSocket(socketState, new SocketIO());
		socket.onConnect(getSession());

		ResponseCallback callback = new ResponseCallback();
		socket.send(1234, "test".getBytes(), callback);

		SocketResponse response = new SocketResponse(1234, SocketResponse.ACK);
		ByteArrayInputStream inputStream = new ByteArrayInputStream(response.getByteBuffer().array());

		assertFalse(callback.hasResponse());

		socket.onMessage(inputStream);

		assertTrue(callback.hasResponse());
		assertTrue(response.isAck());
		assertEquals(response.getId(), callback.getResponses().get(0).getId());
	}

	@Test
	public void testSocketState() throws Exception
	{
		Session session = getSession();
		SocketState socketState = mock(SocketState.class);
		AggregatorSocket socket = new AggregatorSocket(socketState, new SocketIO());

		socket.onConnect(session);
		verify(socketState).onConnect(session);

		String reason = "becase I'm testing it";
		socket.onClose(session, 123, reason);
		verify(socketState).onClose(session, 123, reason);

		IllegalStateException error = new IllegalStateException("I love creating errors");
		socket.onError(session, error);
		verify(socketState).onError(session, error);
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
