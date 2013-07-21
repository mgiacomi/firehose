package com.gltech.scale.core.websocket;

import org.joda.time.DateTime;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SocketRequestTest
{
	@Test
	public void testToAndBack() throws Exception
	{
		SocketRequest to = new SocketRequest(123, 0, "myChannel", DateTime.now(), "I love testing stuff".getBytes());
		SocketRequest back = new SocketRequest(to.getByteBuffer().array());

		assertEquals(to.getId(), back.getId());
		assertEquals(to.isPrimary(), back.isPrimary());
		assertEquals(to.getChannelName(), back.getChannelName());
		assertEquals(to.getPeriod(), back.getPeriod());
		assertEquals(new String(to.getData()), new String(back.getData()));
	}

	@Test
	public void testChannelNameTooBig() throws Exception
	{
		boolean gotException = false;

		try
		{
			new SocketRequest(123, 0, "myChannelWithTooBigOfAName", DateTime.now(), "I love testing stuff".getBytes());
		}
		catch (IllegalArgumentException e)
		{
			gotException = true;
		}

		assertTrue(gotException);

		gotException = false;

		try
		{
			new SocketRequest(123, 3, "myChannel", DateTime.now(), "I love testing stuff".getBytes());
		}
		catch (IllegalArgumentException e)
		{
			gotException = true;
		}

		assertTrue(gotException);
	}
}
