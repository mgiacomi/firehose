package com.gltech.scale.core.storage;

import com.gltech.scale.core.model.ChannelMetaData;
import org.joda.time.DateTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.ws.rs.core.MediaType;

import static junit.framework.Assert.assertEquals;

public class BucketMetaDataTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void testEventSet()
	{
		ChannelMetaData channelMetaData = createEventSetBucket("C", "B");

		String json = "{\"customer\":\"C\",\"bucket\":\"B\",\"bucketType\":\"eventset\",\"mediaType\":\"application/json\",\"lifeTime\":\"small\",\"periodSeconds\":60,\"redundancy\":\"singlewrite\"}";
		assertEquals(json, channelMetaData.toJson().toString());
		assertEquals("BucketMetaData{customer='C', bucket='B' " + json + "}", channelMetaData.toString());
	}

	public static ChannelMetaData createEventSetBucket(String customer, String bucket)
	{
		return new ChannelMetaData(customer, bucket, "{\"bucketType\":\"EvEntSet\", \"redundancy\":\"singlewrite\", \"mediaType\":\"application/json\"}");
	}

	@Test
	public void testBytes()
	{

	}

	@Test
	public void testWrong()
	{
		exception.expect(BucketMetaDataException.class);
		new ChannelMetaData("C", "B", "{\"bucketType\":\"nothing\"}");
	}

	@Test
	public void testNearestPeriodCeiling()
	{
		// 9/21/2012 22:31:42
		DateTime testDate = new DateTime(2012, 9, 21, 22, 31, 41);

		// Periods by minute buckets
		ChannelMetaData oneMinChannel = new ChannelMetaData("c", "b", ChannelMetaData.BucketType.eventset, 60, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
		ChannelMetaData threeMinChannel = new ChannelMetaData("c", "b", ChannelMetaData.BucketType.eventset, 180, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
		ChannelMetaData fiveMinChannel = new ChannelMetaData("c", "b", ChannelMetaData.BucketType.eventset, 300, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
		ChannelMetaData sixMinChannel = new ChannelMetaData("c", "b", ChannelMetaData.BucketType.eventset, 360, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
		ChannelMetaData tenMinChannel = new ChannelMetaData("c", "b", ChannelMetaData.BucketType.eventset, 600, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
		ChannelMetaData thirtyMinChannel = new ChannelMetaData("c", "b", ChannelMetaData.BucketType.eventset, 1800, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
		assertEquals(new DateTime(2012, 9, 21, 22, 32, 0), oneMinChannel.nearestPeriodCeiling(testDate));
		assertEquals(new DateTime(2012, 9, 21, 22, 33, 0), threeMinChannel.nearestPeriodCeiling(testDate));
		assertEquals(new DateTime(2012, 9, 21, 22, 35, 0), fiveMinChannel.nearestPeriodCeiling(testDate));
		assertEquals(new DateTime(2012, 9, 21, 22, 36, 0), sixMinChannel.nearestPeriodCeiling(testDate));
		assertEquals(new DateTime(2012, 9, 21, 22, 40, 0), tenMinChannel.nearestPeriodCeiling(testDate));
		assertEquals(new DateTime(2012, 9, 21, 23, 0, 0), thirtyMinChannel.nearestPeriodCeiling(testDate));

		// Periods by minute buckets
		ChannelMetaData threeSecChannel = new ChannelMetaData("c", "b", ChannelMetaData.BucketType.eventset, 3, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
		ChannelMetaData fiveSecChannel = new ChannelMetaData("c", "b", ChannelMetaData.BucketType.eventset, 5, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
		ChannelMetaData tenSecChannel = new ChannelMetaData("c", "b", ChannelMetaData.BucketType.eventset, 10, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
		ChannelMetaData thirtySecChannel = new ChannelMetaData("c", "b", ChannelMetaData.BucketType.eventset, 30, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
		assertEquals(new DateTime(2012, 9, 21, 22, 31, 42), threeSecChannel.nearestPeriodCeiling(testDate));
		assertEquals(new DateTime(2012, 9, 21, 22, 31, 45), fiveSecChannel.nearestPeriodCeiling(testDate));
		assertEquals(new DateTime(2012, 9, 21, 22, 31, 50), tenSecChannel.nearestPeriodCeiling(testDate));
		assertEquals(new DateTime(2012, 9, 21, 22, 32, 0), thirtySecChannel.nearestPeriodCeiling(testDate));
	}

	@Test
	public void testIsPeriodValid()
	{
		// All divisible by an hour (in seconds)
		new ChannelMetaData("c", "b", ChannelMetaData.BucketType.eventset, 10, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
		new ChannelMetaData("c", "b", ChannelMetaData.BucketType.eventset, 20, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
		new ChannelMetaData("c", "b", ChannelMetaData.BucketType.eventset, 30, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
		new ChannelMetaData("c", "b", ChannelMetaData.BucketType.eventset, 60, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
		new ChannelMetaData("c", "b", ChannelMetaData.BucketType.eventset, 120, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
		new ChannelMetaData("c", "b", ChannelMetaData.BucketType.eventset, 180, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
		new ChannelMetaData("c", "b", ChannelMetaData.BucketType.eventset, 300, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
		new ChannelMetaData("c", "b", ChannelMetaData.BucketType.eventset, 360, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
	}

	@Test
	public void testIsPeriodValidFail11Seconds()
	{
		exception.expect(IllegalArgumentException.class);
		new ChannelMetaData("c", "b", ChannelMetaData.BucketType.eventset, 11, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
	}

	@Test
	public void testIsPeriodValidFail35Seconds()
	{
		exception.expect(IllegalArgumentException.class);
		new ChannelMetaData("c", "b", ChannelMetaData.BucketType.eventset, 35, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
	}

	@Test
	public void testIsPeriodValidFail130Seconds()
	{
		exception.expect(IllegalArgumentException.class);
		new ChannelMetaData("c", "b", ChannelMetaData.BucketType.eventset, 130, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
	}

	@Test
	public void testIsPeriodValidFail320Seconds()
	{
		exception.expect(IllegalArgumentException.class);
		new ChannelMetaData("c", "b", ChannelMetaData.BucketType.eventset, 320, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
	}
}
