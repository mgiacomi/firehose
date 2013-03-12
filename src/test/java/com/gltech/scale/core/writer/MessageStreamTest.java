package com.gltech.scale.core.writer;

import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.core.model.Message;
import com.gltech.scale.core.rope.TimeBucket;
import com.gltech.scale.core.storage.BucketMetaData;
import org.joda.time.DateTime;
import org.junit.Test;

import javax.ws.rs.core.MediaType;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

public class MessageStreamTest
{
	@Test
	public void nextRecordTest() throws Exception
	{
		BucketMetaData bucketMetaData = new BucketMetaData("1", "2", BucketMetaData.BucketType.eventset, 15, MediaType.APPLICATION_OCTET_STREAM_TYPE, BucketMetaData.LifeTime.medium, BucketMetaData.Redundancy.doublewritesync);

		TimeBucket timeBucket = new TimeBucket(bucketMetaData, TimePeriodUtils.nearestPeriodCeiling(DateTime.now(), 5));
		timeBucket.addEvent(new Message("1", "2", "testdata".getBytes()));
		Thread.sleep(5);
		timeBucket.addEvent(new Message("3", "4", "testdata2".getBytes()));

		assertEquals("testdata", new String(timeBucket.getEvents().get(0).getPayload()));
		assertEquals("testdata2", new String(timeBucket.getEvents().get(1).getPayload()));

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		timeBucket.eventsToJson(bos);

		MessageStream messageStream = new MessageInputStream("test", new ByteArrayInputStream(bos.toByteArray()));

		assertEquals("testdata", new String(messageStream.getCurrentMessage().getPayload()));
		messageStream.nextRecord();
		assertEquals("testdata2", new String(messageStream.getCurrentMessage().getPayload()));
		messageStream.nextRecord();
		assertNull(messageStream.getCurrentMessage());
	}
}
