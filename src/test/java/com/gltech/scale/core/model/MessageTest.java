package com.gltech.scale.core.model;

import com.dyuproject.protostuff.JsonIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.ExplicitIdStrategy;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import org.junit.Test;

import javax.ws.rs.core.MediaType;

import static junit.framework.Assert.*;

public class MessageTest
{
	@Test
	public void testToJsonAndBack() throws Exception{
		ExplicitIdStrategy.Registry registry = new ExplicitIdStrategy.Registry();
		registry.registerPojo(Message.class, 1);
		Schema<Message> schema = RuntimeSchema.getSchema(Message.class);

		Message before = new Message(MediaType.APPLICATION_JSON_TYPE, "my test string".getBytes());
		byte[] json = JsonIOUtil.toByteArray(before, schema, false);

		Message after = new Message();
		JsonIOUtil.mergeFrom(json, after, schema, false);

		assertEquals(before.getUuid(), after.getUuid());
		assertEquals(before.getMimeTypeId(), after.getMimeTypeId());
		assertEquals(new String(before.getPayload()), new String(after.getPayload()));
		assertEquals(before.getReceived_at().getMillis(), after.getReceived_at().getMillis());
	}
}
