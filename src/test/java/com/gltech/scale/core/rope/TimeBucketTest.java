package com.gltech.scale.core.rope;

import com.gltech.scale.core.coordination.TimePeriodUtils;
import com.gltech.scale.core.event.EventPayload;
import com.gltech.scale.core.storage.BucketMetaData;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.MediaType;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;

import static junit.framework.Assert.assertEquals;

public class TimeBucketTest
{
	private TimeBucket timeBucket;
	private BucketMetaData bucketMetaData;

	@Before
	public void setUp() throws Exception
	{
		bucketMetaData = new BucketMetaData("1", "2", BucketMetaData.BucketType.eventset, 15, MediaType.APPLICATION_OCTET_STREAM_TYPE, BucketMetaData.LifeTime.medium, BucketMetaData.Redundancy.doublewritesync);

		timeBucket = new TimeBucket(bucketMetaData, TimePeriodUtils.nearestPeriodCeiling(DateTime.now(), 5));
		timeBucket.addEvent(new EventPayload("1", "2", "testdata".getBytes()));
		timeBucket.addEvent(new EventPayload("3", "4", "testdata2".getBytes()));
	}


	@Test
	public void testToJsonAndBack() throws Exception
	{
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		timeBucket.toJson(bos);

		System.out.println(bos.toString());

		TimeBucket tb1 = new TimeBucket(new ByteArrayInputStream(bos.toByteArray()));

		assertEquals(timeBucket.getBucketMetaData(), tb1.getBucketMetaData());
		assertEquals(timeBucket.getBucketMetaData().getRedundancy(), tb1.getBucketMetaData().getRedundancy());
		assertEquals(timeBucket.getLastEventTime(), tb1.getLastEventTime());
		assertEquals(timeBucket.getBytes(), tb1.getBytes());
		assertEquals(timeBucket.getEvents().size(), tb1.getEvents().size());

		for (int i = 0; i < timeBucket.getEvents().size(); i++)
		{
			EventPayload e1 = timeBucket.getEvents().get(i);
			EventPayload e2 = tb1.getEvents().get(i);
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
		TimeBucket bigTimeBucket = new TimeBucket(bucketMetaData, TimePeriodUtils.nearestPeriodCeiling(DateTime.now(), 5));

		for (int i = 0; i < 100000; i++)
		{
			String testString = i + "asdf123asdf123asdf123asdf132asdf132asdf132a1sdf321asdf312adsf31asdf312adsf31asdf31asd";
			bigTimeBucket.addEvent(new EventPayload("1", "2", testString.getBytes()));
		}

		long timer = System.currentTimeMillis();
		bigTimeBucket.toJson(new FileOutputStream(new File("stream.json")));
		System.out.println(bigTimeBucket.getEvents().size() + " items to Json string in " + (System.currentTimeMillis() - timer) + "ms");
	}
}
