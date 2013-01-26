package com.gltech.scale.core.storage;

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
		BucketMetaData bucketMetaData = createEventSetBucket("C", "B");

		String json = "{\"customer\":\"C\",\"bucket\":\"B\",\"bucketType\":\"eventset\",\"mediaType\":\"application/json\",\"lifeTime\":\"small\",\"periodSeconds\":60,\"redundancy\":\"singlewrite\"}";
		assertEquals(json, bucketMetaData.toJson().toString());
		assertEquals("BucketMetaData{customer='C', bucket='B' " + json + "}", bucketMetaData.toString());
	}

	public static BucketMetaData createEventSetBucket(String customer, String bucket)
	{
		return new BucketMetaData(customer, bucket, "{\"bucketType\":\"EvEntSet\", \"redundancy\":\"singlewrite\", \"mediaType\":\"application/json\"}");
	}

	@Test
	public void testBytes()
	{

	}

	@Test
	public void testWrong()
	{
		exception.expect(BucketMetaDataException.class);
		new BucketMetaData("C", "B", "{\"bucketType\":\"nothing\"}");
	}

	@Test
	public void testNearestPeriodCeiling()
	{
		// 9/21/2012 22:31:42
		DateTime testDate = new DateTime(2012, 9, 21, 22, 31, 41);

		// Periods by minute buckets
		BucketMetaData oneMinBucket = new BucketMetaData("c", "b", BucketMetaData.BucketType.eventset, 60, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.singlewrite);
		BucketMetaData threeMinBucket = new BucketMetaData("c", "b", BucketMetaData.BucketType.eventset, 180, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.singlewrite);
		BucketMetaData fiveMinBucket = new BucketMetaData("c", "b", BucketMetaData.BucketType.eventset, 300, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.singlewrite);
		BucketMetaData sixMinBucket = new BucketMetaData("c", "b", BucketMetaData.BucketType.eventset, 360, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.singlewrite);
		BucketMetaData tenMinBucket = new BucketMetaData("c", "b", BucketMetaData.BucketType.eventset, 600, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.singlewrite);
		BucketMetaData thirtyMinBucket = new BucketMetaData("c", "b", BucketMetaData.BucketType.eventset, 1800, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.singlewrite);
		assertEquals(new DateTime(2012, 9, 21, 22, 32, 0), oneMinBucket.nearestPeriodCeiling(testDate));
		assertEquals(new DateTime(2012, 9, 21, 22, 33, 0), threeMinBucket.nearestPeriodCeiling(testDate));
		assertEquals(new DateTime(2012, 9, 21, 22, 35, 0), fiveMinBucket.nearestPeriodCeiling(testDate));
		assertEquals(new DateTime(2012, 9, 21, 22, 36, 0), sixMinBucket.nearestPeriodCeiling(testDate));
		assertEquals(new DateTime(2012, 9, 21, 22, 40, 0), tenMinBucket.nearestPeriodCeiling(testDate));
		assertEquals(new DateTime(2012, 9, 21, 23, 0, 0), thirtyMinBucket.nearestPeriodCeiling(testDate));

		// Periods by minute buckets
		BucketMetaData threeSecBucket = new BucketMetaData("c", "b", BucketMetaData.BucketType.eventset, 3, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.singlewrite);
		BucketMetaData fiveSecBucket = new BucketMetaData("c", "b", BucketMetaData.BucketType.eventset, 5, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.singlewrite);
		BucketMetaData tenSecBucket = new BucketMetaData("c", "b", BucketMetaData.BucketType.eventset, 10, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.singlewrite);
		BucketMetaData thirtySecBucket = new BucketMetaData("c", "b", BucketMetaData.BucketType.eventset, 30, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.singlewrite);
		assertEquals(new DateTime(2012, 9, 21, 22, 31, 42), threeSecBucket.nearestPeriodCeiling(testDate));
		assertEquals(new DateTime(2012, 9, 21, 22, 31, 45), fiveSecBucket.nearestPeriodCeiling(testDate));
		assertEquals(new DateTime(2012, 9, 21, 22, 31, 50), tenSecBucket.nearestPeriodCeiling(testDate));
		assertEquals(new DateTime(2012, 9, 21, 22, 32, 0), thirtySecBucket.nearestPeriodCeiling(testDate));
	}

	@Test
	public void testIsPeriodValid()
	{
		// All divisible by an hour (in seconds)
		new BucketMetaData("c", "b", BucketMetaData.BucketType.eventset, 10, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.singlewrite);
		new BucketMetaData("c", "b", BucketMetaData.BucketType.eventset, 20, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.singlewrite);
		new BucketMetaData("c", "b", BucketMetaData.BucketType.eventset, 30, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.singlewrite);
		new BucketMetaData("c", "b", BucketMetaData.BucketType.eventset, 60, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.singlewrite);
		new BucketMetaData("c", "b", BucketMetaData.BucketType.eventset, 120, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.singlewrite);
		new BucketMetaData("c", "b", BucketMetaData.BucketType.eventset, 180, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.singlewrite);
		new BucketMetaData("c", "b", BucketMetaData.BucketType.eventset, 300, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.singlewrite);
		new BucketMetaData("c", "b", BucketMetaData.BucketType.eventset, 360, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.singlewrite);
	}

	@Test
	public void testIsPeriodValidFail11Seconds()
	{
		exception.expect(IllegalArgumentException.class);
		new BucketMetaData("c", "b", BucketMetaData.BucketType.eventset, 11, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.singlewrite);
	}

	@Test
	public void testIsPeriodValidFail35Seconds()
	{
		exception.expect(IllegalArgumentException.class);
		new BucketMetaData("c", "b", BucketMetaData.BucketType.eventset, 35, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.singlewrite);
	}

	@Test
	public void testIsPeriodValidFail130Seconds()
	{
		exception.expect(IllegalArgumentException.class);
		new BucketMetaData("c", "b", BucketMetaData.BucketType.eventset, 130, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.singlewrite);
	}

	@Test
	public void testIsPeriodValidFail320Seconds()
	{
		exception.expect(IllegalArgumentException.class);
		new BucketMetaData("c", "b", BucketMetaData.BucketType.eventset, 320, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.singlewrite);
	}
}
