package com.gltech.scale.core.websocket;

import static org.junit.Assert.*;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;

public class SocketResponseTest
{
	@Test
	public void testIsAck() throws Exception
	{
		SocketResponse response = toAndBack(new SocketResponse(1234, SocketResponse.ACK));
		assertEquals(1234, response.getId());
		assertTrue(response.isAck());
	}

	@Test
	public void testIsError() throws Exception
	{
		String errorMessage = "You Null Ass.";
		NullPointerException nullPointerException = new NullPointerException(errorMessage);
		SocketResponse response = toAndBack(new SocketResponse(1234, SocketResponse.ERROR, nullPointerException.getMessage().getBytes()));
		assertEquals(1234, response.getId());
		assertTrue(response.isError());
		assertEquals(errorMessage, new String(response.getData()));
	}

	@Test
	public void testIsData() throws Exception
	{
		// String
		String data = "Process finished with exit code 0";
		SocketResponse response = toAndBack(new SocketResponse(1234, SocketResponse.DATA, data.getBytes()));
		assertEquals(1234, response.getId());
		assertTrue(response.isData());
		assertEquals(data, new String(response.getData()));

		// float
		float fdata = -92873492834.92734982734f;
		ByteBuffer buffer = ByteBuffer.allocate(4);
		buffer.putFloat(fdata);
		response = toAndBack(new SocketResponse(1234, SocketResponse.DATA, buffer.array()));
		assertEquals(1234, response.getId());
		assertTrue(response.isData());
		assertEquals(fdata, ByteBuffer.wrap(response.getData()).getFloat(), 1e-15);
	}

	private SocketResponse toAndBack(SocketResponse response)
	{
		return new SocketResponse(response.getByteBuffer().array());
	}
}
