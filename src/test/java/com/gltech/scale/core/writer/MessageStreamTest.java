package com.gltech.scale.core.writer;

import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.core.model.Message;
import com.gltech.scale.core.model.Batch;
import com.gltech.scale.core.model.ChannelMetaData;
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
		ChannelMetaData channelMetaData = new ChannelMetaData("1", 0, true);

		Batch batch = new Batch(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(DateTime.now(), 5));
		batch.addEvent(new Message(MediaType.APPLICATION_JSON_TYPE, "testdata".getBytes()));
		Thread.sleep(5);
		batch.addEvent(new Message(MediaType.APPLICATION_JSON_TYPE, "testdata2".getBytes()));

		assertEquals("testdata", new String(batch.getEvents().get(0).getPayload()));
		assertEquals("testdata2", new String(batch.getEvents().get(1).getPayload()));

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		batch.eventsToJson(bos);

		MessageStream messageStream = new MessageInputStream("test", new ByteArrayInputStream(bos.toByteArray()));

		assertEquals("testdata", new String(messageStream.getCurrentMessage().getPayload()));
		messageStream.nextRecord();
		assertEquals("testdata2", new String(messageStream.getCurrentMessage().getPayload()));
		messageStream.nextRecord();
		assertNull(messageStream.getCurrentMessage());
	}
}
