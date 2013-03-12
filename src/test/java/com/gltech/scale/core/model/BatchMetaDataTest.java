package com.gltech.scale.core.model;

import com.gltech.scale.core.model.BatchMetaData;
import com.gltech.scale.core.model.ChannelMetaData;
import org.joda.time.DateTime;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class BatchMetaDataTest
{
	@Test
	public void testToJsonAndBack() throws Exception
	{
		ChannelMetaData channelMetaData = new ChannelMetaData("C", "B", "{\"bucketType\":\"EvEntSet\", \"redundancy\":\"singlewrite\", \"mediaType\":\"application/json\"}");
		BatchMetaData tbmd1 = new BatchMetaData(DateTime.now(), 0, 0, channelMetaData);

		String json = tbmd1.toJson().toString();
		BatchMetaData tbmd2 = new BatchMetaData(json);

		assertEquals(tbmd1, tbmd2);
	}
}