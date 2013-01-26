package com.gltech.scale.core.event;

import org.junit.Test;

import static junit.framework.Assert.*;

public class EventPayloadTest
{
	@Test
	public void testToJsonAndBack() throws Exception
	{
		EventPayload ep = new EventPayload("1", "2", "[\"data\": \"test\"]".getBytes());
		String json = ep.toJson().toString();

		EventPayload ep1 = new EventPayload(json);

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
		EventPayload ep = new EventPayload("1", "2");
		String json = ep.toJson().toString();

		EventPayload ep1 = new EventPayload(json);

		assertEquals(ep.getCustomer(), ep1.getCustomer());
		assertEquals(ep.getBucket(), ep1.getBucket());
		assertEquals(ep.getUuid(), ep1.getUuid());
		assertEquals(ep.getReceived_at(), ep1.getReceived_at());
		assertEquals("", new String(ep.getPayload()));
		assertTrue(ep.isStored());
	}
}
