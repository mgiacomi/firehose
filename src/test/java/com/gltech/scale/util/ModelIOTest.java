package com.gltech.scale.util;

import com.gltech.scale.core.model.Message;

import static junit.framework.Assert.*;

import org.junit.Test;

import javax.ws.rs.core.MediaType;

public class ModelIOTest
{
	@Test
	public void testMessage()
	{
		ModelIO modelIO = new ModelIO();
		Message before = new Message(MediaType.APPLICATION_JSON_TYPE, "message test 1".getBytes());
		String messageJson = modelIO.toJson(before);
		Message after = modelIO.toMessage(messageJson);
		assertEquals(before, after);
		byte[] messageBytes = modelIO.toBytes(after);
		after = modelIO.toMessage(messageBytes);
		assertEquals(before, after);
	}
}
