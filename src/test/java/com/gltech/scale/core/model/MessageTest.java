package com.gltech.scale.core.model;

import com.gltech.scale.core.model.Message;
import org.junit.Test;

import static junit.framework.Assert.*;

public class MessageTest
{
/*
	@Test
	public void testToJsonAndBack() throws Exception
	{
		Message ep = new Message("1", "2", "[\"data\": \"test\"]".getBytes());
		String json = ep.toJson().toString();

		Message ep1 = new Message(json);

		assertEquals(ep.getCustomer(), ep1.getCustomer());
		assertEquals(ep.getBucket(), ep1.getBucket());
		assertEquals(ep.getUuid(), ep1.getUuid());
		assertEquals(ep.getReceived_at(), ep1.getReceived_at());
		assertEquals(new String(ep.getPayload()), new String(ep1.getPayload()));
		assertFalse(ep.isStored());
	}

	@Test
	public void testToJsonAndBackStored() throws Exception
	{
		Message ep = new Message("1", "2");
		String json = ep.toJson().toString();

		Message ep1 = new Message(json);

		assertEquals(ep.getCustomer(), ep1.getCustomer());
		assertEquals(ep.getBucket(), ep1.getBucket());
		assertEquals(ep.getUuid(), ep1.getUuid());
		assertEquals(ep.getReceived_at(), ep1.getReceived_at());
		assertEquals("", new String(ep.getPayload()));
		assertTrue(ep.isStored());
	}
*/
}
