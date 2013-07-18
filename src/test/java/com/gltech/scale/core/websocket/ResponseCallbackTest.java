package com.gltech.scale.core.websocket;

import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResponseCallbackTest
{
	UUID p_uuid = UUID.randomUUID();
	UUID s_uuid = UUID.randomUUID();
	ServiceMetaData primary = mock(ServiceMetaData.class);
	ServiceMetaData backup = mock(ServiceMetaData.class);

	@Before
	public void setUp() throws Exception
	{
		when(primary.getWorkerId()).thenReturn(p_uuid);
		when(backup.getWorkerId()).thenReturn(s_uuid);
	}

	@Test
	public void testHasResponse() throws Exception
	{
		ResponseCallback responseCallback = new ResponseCallback(primary, backup);
		assertFalse(responseCallback.hasResponse());
		responseCallback.setResponse(p_uuid, new SocketResponse(123, SocketResponse.ACK));
		assertTrue(responseCallback.hasResponse());

		responseCallback = new ResponseCallback(primary, backup);
		assertFalse(responseCallback.hasResponse());
		responseCallback.setResponse(s_uuid, new SocketResponse(123, SocketResponse.ACK));
		assertTrue(responseCallback.hasResponse());
	}

	@Test
	public void testGotAck() throws Exception
	{
		ResponseCallback responseCallback = new ResponseCallback(primary, backup);
		assertFalse(responseCallback.hasResponse());
		responseCallback.setResponse(p_uuid, new SocketResponse(123, SocketResponse.ACK));
		assertTrue(responseCallback.hasResponse());
		assertTrue(responseCallback.gotAck());
	}

	@Test
	public void testLogErrors() throws Exception
	{
		ResponseCallback responseCallback = new ResponseCallback(primary, backup);
		assertFalse(responseCallback.hasResponse());
		responseCallback.setRequestFuture(p_uuid, new Future<Void>() {
			public boolean cancel(boolean mayInterruptIfRunning)
			{
				return false;
			}

			public boolean isCancelled()
			{
				return false;
			}

			public boolean isDone()
			{
				return false;
			}

			public Void get() throws InterruptedException, ExecutionException
			{
				return null;
			}

			public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
			{
				return null;
			}
		});

		responseCallback.setResponse(p_uuid, new SocketResponse(123, SocketResponse.ERROR, "BadError".getBytes()));
		assertTrue(responseCallback.hasResponse());
		assertTrue(responseCallback.logErrors());


		responseCallback = new ResponseCallback(primary, backup);
		assertFalse(responseCallback.hasResponse());
		responseCallback.setRequestFuture(p_uuid, new Future<Void>() {
			public boolean cancel(boolean mayInterruptIfRunning)
			{
				return false;
			}

			public boolean isCancelled()
			{
				return false;
			}

			public boolean isDone()
			{
				return false;
			}

			public Void get() throws InterruptedException, ExecutionException
			{
				return null;
			}

			public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
			{
				throw new RuntimeException("Future Failure");
			}
		});

		responseCallback.setResponse(p_uuid, new SocketResponse(123, SocketResponse.ACK));
		assertTrue(responseCallback.hasResponse());
		assertTrue(responseCallback.logErrors());
	}
}
