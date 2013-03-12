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
		ChannelMetaData channelMetaData = new ChannelMetaData("1", "2", ChannelMetaData.BucketType.eventset, 15, MediaType.APPLICATION_OCTET_STREAM_TYPE, ChannelMetaData.LifeTime.medium, ChannelMetaData.Redundancy.doublewritesync);

		Message e1 = new Message("C1", "B", "testdata0".getBytes());
		Thread.sleep(10);
		Message e2 = new Message("C2", "B", "testdata1".getBytes());
		Thread.sleep(10);
		Message e3 = new Message("C3", "B", "testdata2".getBytes());
		Thread.sleep(10);
		Message e4 = new Message("C4", "B", "testdata3".getBytes());
		Thread.sleep(10);
		Message e5 = new Message("C5", "B", "testdata4".getBytes());
		Thread.sleep(10);
		Message e6 = new Message("C6", "B", "testdata5".getBytes());
		Thread.sleep(10);
		Message e7 = new Message("C7", "B", "testdata6".getBytes());
		Thread.sleep(10);
		Message e8 = new Message("C8", "B", "testdata7".getBytes());
		Thread.sleep(10);
		Message e9 = new Message("C9", "B", "testdata8".getBytes());

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