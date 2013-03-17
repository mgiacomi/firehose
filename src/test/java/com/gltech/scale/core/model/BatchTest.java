package com.gltech.scale.core.model;

import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.core.model.Message;
import com.gltech.scale.core.model.Batch;
import com.gltech.scale.core.model.ChannelMetaData;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.MediaType;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;

import static junit.framework.Assert.assertEquals;

public class BatchTest
{
/*
	private Batch batch;
	private ChannelMetaData channelMetaData;

	@Before
	public void setUp() throws Exception
	{
		channelMetaData = new ChannelMetaData("1", "2", ChannelMetaData.BucketType.eventset, 15, MediaType.APPLICATION_OCTET_STREAM_TYPE, ChannelMetaData.LifeTime.medium, ChannelMetaData.Redundancy.doublewritesync);

		batch = new Batch(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(DateTime.now(), 5));
		batch.addMessage(new Message("1", "2", "testdata".getBytes()));
		batch.addMessage(new Message("3", "4", "testdata2".getBytes()));
	}


	@Test
	public void testToJsonAndBack() throws Exception
	{
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		batch.toJson(bos);

		System.out.println(bos.toString());

		Batch tb1 = new Batch(new ByteArrayInputStream(bos.toByteArray()));

		assertEquals(batch.getChannelMetaData(), tb1.getChannelMetaData());
		assertEquals(batch.getChannelMetaData().getRedundancy(), tb1.getChannelMetaData().getRedundancy());
		assertEquals(batch.getLastMessageTime(), tb1.getLastMessageTime());
		assertEquals(batch.getBytes(), tb1.getBytes());
		assertEquals(batch.getMessages().size(), tb1.getMessages().size());

		for (int i = 0; i < batch.getMessages().size(); i++)
		{
			Message e1 = batch.getMessages().get(i);
			Message e2 = tb1.getMessages().get(i);
			assertEquals(e1.getCustomer(), e2.getCustomer());
			assertEquals(e1.getBucket(), e2.getBucket());
			assertEquals(e1.getUuid(), e2.getUuid());
			assertEquals(e1.getReceived_at(), e2.getReceived_at());
			assertEquals(new String(e1.getPayload()), new String(e2.getPayload()));
		}
	}


	@Test
	public void testSpeedToJsonAndBack100k() throws Exception
	{
		Batch bigBatch = new Batch(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(DateTime.now(), 5));

		for (int i = 0; i < 100000; i++)
		{
			String testString = i + "asdf123asdf123asdf123asdf132asdf132asdf132a1sdf321asdf312adsf31asdf312adsf31asdf31asd";
			bigBatch.addMessage(new Message("1", "2", testString.getBytes()));
		}

		long timer = System.currentTimeMillis();
		bigBatch.toJson(new FileOutputStream(new File("stream.json")));
		System.out.println(bigBatch.getMessages().size() + " items to Json string in " + (System.currentTimeMillis() - timer) + "ms");
	}
*/
}
