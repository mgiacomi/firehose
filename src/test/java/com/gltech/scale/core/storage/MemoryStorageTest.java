package com.gltech.scale.core.storage;

import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.storage.bytearray.MemoryStorage;
import com.gltech.scale.core.storage.bytearray.ByteArrayStorage;
import com.gltech.scale.core.storage.bytearray.StoragePayload;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class MemoryStorageTest
{
	/*
	private ByteArrayStorage byteArrayStorage;
	private ChannelMetaData channelC1B1;
	private ChannelMetaData channelC1B2;
	private ChannelMetaData channelC2B1;

	@Before
	public void setUp() throws Exception
	{
		byteArrayStorage = new MemoryStorage();
		channelC1B1 = BucketMetaDataTest.createEventSetBucket("C1", "B1");
		channelC1B2 = BucketMetaDataTest.createEventSetBucket("C1", "B2");
		channelC2B1 = BucketMetaDataTest.createEventSetBucket("C2", "B1");
	}

	@Test
	public void testMultiples()
	{
		byteArrayStorage.putBucket(channelC1B1);
		byteArrayStorage.putBucket(channelC1B2);
		byteArrayStorage.putBucket(channelC2B1);

		StoragePayload A_C1_B1 = new StoragePayload("A", "C1", "B1", "payload A".getBytes());
		byteArrayStorage.putPayload(A_C1_B1);

		StoragePayload B_C1_B1 = new StoragePayload("B", "C1", "B1", "payload B".getBytes());
		byteArrayStorage.putPayload(B_C1_B1);

		StoragePayload C_C1_B2 = new StoragePayload("C", "C1", "B2", "payload C".getBytes());
		byteArrayStorage.putPayload(C_C1_B2);

		StoragePayload D_C2_B1 = new StoragePayload("D", "C2", "B1", "payload D".getBytes());
		byteArrayStorage.putPayload(D_C2_B1);

		assertArrayEquals(A_C1_B1.getPayload(), byteArrayStorage.getPayload("1", "A").getPayload());
		assertArrayEquals(B_C1_B1.getPayload(), byteArrayStorage.getPayload("1", "B").getPayload());
		assertArrayEquals(C_C1_B2.getPayload(), byteArrayStorage.getPayload("1", "C").getPayload());
		assertArrayEquals(D_C2_B1.getPayload(), byteArrayStorage.getPayload("1", "D").getPayload());

	}

	@Test(expected = StorageException.class)
	public void testMultipleBucketCreation()
	{
		ChannelMetaData channelC1B1 = BucketMetaDataTest.createEventSetBucket("C1", "B1");
		byteArrayStorage.putBucket(channelC1B1);
		byteArrayStorage.putBucket(channelC1B1);
	}
*/
}
