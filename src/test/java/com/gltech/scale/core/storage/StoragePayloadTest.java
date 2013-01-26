package com.gltech.scale.core.storage;

import com.gltech.scale.core.storage.bytearray.StoragePayload;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;

public class StoragePayloadTest
{
	@Test
	public void testCycle() throws IOException
	{
		StoragePayload payload = new StoragePayload("ABC", "cust", "buck", "bytes bytes bytes".getBytes());
		StoragePayload cycled = StoragePayload.convert(payload.convert());
		assertEquals(payload.getId(), cycled.getId());
		assertEquals(payload.getCustomer(), cycled.getCustomer());
		assertEquals(payload.getBucket(), cycled.getBucket());
		assertEquals(new String(payload.getPayload()), new String(cycled.getPayload()));
	}
}
