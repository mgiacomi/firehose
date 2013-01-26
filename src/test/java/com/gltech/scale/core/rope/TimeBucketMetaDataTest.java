package com.gltech.scale.core.rope;

import com.gltech.scale.core.storage.BucketMetaData;
import org.joda.time.DateTime;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class TimeBucketMetaDataTest
{
	@Test
	public void testToJsonAndBack() throws Exception
	{
		BucketMetaData bucketMetaData = new BucketMetaData("C", "B", "{\"bucketType\":\"EvEntSet\", \"redundancy\":\"singlewrite\", \"mediaType\":\"application/json\"}");
		TimeBucketMetaData tbmd1 = new TimeBucketMetaData(DateTime.now(), 0, 0, bucketMetaData);

		String json = tbmd1.toJson().toString();
		TimeBucketMetaData tbmd2 = new TimeBucketMetaData(json);

		assertEquals(tbmd1, tbmd2);
	}
}