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
import java.util.List;

import static junit.framework.Assert.assertEquals;

public class BatchStreamsManagerTest
{
	@Test
	public void nextMultiStreamTest() throws Exception
	{
		ChannelMetaData channelMetaData = new ChannelMetaData("test", 3, false);

		Message e1 = new Message(MediaType.APPLICATION_JSON_TYPE, "testdata0".getBytes());
		Thread.sleep(10);
		Message e2 = new Message(MediaType.APPLICATION_JSON_TYPE, "testdata1".getBytes());
		Thread.sleep(10);
		Message e3 = new Message(MediaType.APPLICATION_JSON_TYPE, "testdata2".getBytes());
		Thread.sleep(10);
		Message e4 = new Message(MediaType.APPLICATION_JSON_TYPE, "testdata3".getBytes());
		Thread.sleep(10);
		Message e5 = new Message(MediaType.APPLICATION_JSON_TYPE, "testdata4".getBytes());
		Thread.sleep(10);
		Message e6 = new Message(MediaType.APPLICATION_JSON_TYPE, "testdata5".getBytes());
		Thread.sleep(10);
		Message e7 = new Message(MediaType.APPLICATION_JSON_TYPE, "testdata6".getBytes());
		Thread.sleep(10);
		Message e8 = new Message(MediaType.APPLICATION_JSON_TYPE, "testdata7".getBytes());
		Thread.sleep(10);
		Message e9 = new Message(MediaType.APPLICATION_JSON_TYPE, "testdata8".getBytes());

		DateTime period = DateTime.now();

		Batch batch1 = new Batch(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(period, 5));
		Batch batch2 = new Batch(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(period, 5));
		Batch timeBucketb1 = new Batch(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(period, 5));
		Batch timeBucketb2 = new Batch(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(period, 5));

		batch1.addEvent(e3);
		batch1.addEvent(e5);
		batch1.addEvent(e6);
		batch1.addEvent(e9);

		batch2.addEvent(e1);
		batch2.addEvent(e2);
		batch2.addEvent(e4);
		batch2.addEvent(e7);
		batch2.addEvent(e8);

		timeBucketb1.addEvent(e2);
		timeBucketb1.addEvent(e3);
		timeBucketb1.addEvent(e7);

		timeBucketb2.addEvent(e1);
		timeBucketb2.addEvent(e4);
		timeBucketb2.addEvent(e5);
		timeBucketb2.addEvent(e6);
		timeBucketb2.addEvent(e8);
		timeBucketb2.addEvent(e9);

		ByteArrayOutputStream bos1 = new ByteArrayOutputStream();
		ByteArrayOutputStream bos2 = new ByteArrayOutputStream();
		ByteArrayOutputStream bos3 = new ByteArrayOutputStream();
		ByteArrayOutputStream bos4 = new ByteArrayOutputStream();

		batch1.eventsToJson(bos1);
		batch2.eventsToJson(bos2);
		timeBucketb1.eventsToJson(bos3);
		timeBucketb2.eventsToJson(bos4);

		BatchStreamsManager batchStreamsManager = new BatchStreamsManager(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(period, 5));
		batchStreamsManager.registerInputStream(new ByteArrayInputStream(bos1.toByteArray()));
		batchStreamsManager.registerInputStream(new ByteArrayInputStream(bos2.toByteArray()));
		batchStreamsManager.registerInputStream(new ByteArrayInputStream(bos3.toByteArray()));
		batchStreamsManager.registerInputStream(new ByteArrayInputStream(bos4.toByteArray()));

		ByteArrayOutputStream storageStream = new ByteArrayOutputStream();
		batchStreamsManager.writeEvents(storageStream);

		ByteArrayInputStream eventsStream = new ByteArrayInputStream(storageStream.toByteArray());
		List<Message> events = Batch.jsonToEvents(eventsStream);

		for (int i = 0; i < 9; i++)
		{
			assertEquals("testdata" + i, new String(events.get(i).getPayload()));
		}
	}
}
