package com.gltech.scale.core.model;

import com.gltech.scale.core.model.Message;

import static junit.framework.Assert.*;

import com.gltech.scale.core.model.ModelIO;
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

	@Test
	public void testNullBatchMetaData()
	{
		ModelIO modelIO = new ModelIO();
		ChannelMetaData channelMetaData = null;
		modelIO.toBytes(channelMetaData);

	}
}
