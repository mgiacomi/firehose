package com.gltech.scale.core.writer;

import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import com.gltech.scale.core.aggregator.BatchMemory;
import com.gltech.scale.core.aggregator.BatchNIOFile;
import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.core.model.Message;
import com.gltech.scale.core.aggregator.Batch;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.model.ModelIO;
import org.joda.time.DateTime;
import org.junit.Test;

import javax.ws.rs.core.MediaType;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static junit.framework.Assert.assertEquals;

public class BatchStreamsManagerTest
{
	private ChannelMetaData channelMetaData = new ChannelMetaData("test", ChannelMetaData.TTL_DAY, false);
	DateTime period = DateTime.now();

	@Test
	public void nextMultiStreamMemoryTest() throws Exception
	{
		Batch batch1 = new BatchMemory(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(period, 5));
		Batch batch2 = new BatchMemory(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(period, 5));
		Batch batch3 = new BatchMemory(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(period, 5));
		Batch batch4 = new BatchMemory(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(period, 5));
		nextMultiStreamTest(batch1, batch2, batch3, batch4);
	}

	@Test
	public void nextMultiStreamNIOFileTest() throws Exception
	{
		Batch batch1 = new BatchNIOFile(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(period, 5));
		Batch batch2 = new BatchNIOFile(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(period, 5));
		Batch batch3 = new BatchNIOFile(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(period, 5));
		Batch batch4 = new BatchNIOFile(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(period, 5));
		nextMultiStreamTest(batch1, batch2, batch3, batch4);
	}

	private void nextMultiStreamTest(Batch batch1, Batch batch2, Batch batch3, Batch batch4) throws Exception
	{
		ModelIO modelIO = new ModelIO();
		Schema<Message> schema = RuntimeSchema.getSchema(Message.class);

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

		batch1.addMessage(modelIO.toBytes(e3));
		batch1.addMessage(modelIO.toBytes(e5));
		batch1.addMessage(modelIO.toBytes(e6));
		batch1.addMessage(modelIO.toBytes(e9));

		batch2.addMessage(modelIO.toBytes(e1));
		batch2.addMessage(modelIO.toBytes(e2));
		batch2.addMessage(modelIO.toBytes(e4));
		batch2.addMessage(modelIO.toBytes(e7));
		batch2.addMessage(modelIO.toBytes(e8));

		batch3.addMessage(modelIO.toBytes(e2));
		batch3.addMessage(modelIO.toBytes(e3));
		batch3.addMessage(modelIO.toBytes(e7));

		batch4.addMessage(modelIO.toBytes(e1));
		batch4.addMessage(modelIO.toBytes(e4));
		batch4.addMessage(modelIO.toBytes(e5));
		batch4.addMessage(modelIO.toBytes(e6));
		batch4.addMessage(modelIO.toBytes(e8));
		batch4.addMessage(modelIO.toBytes(e9));

		ByteArrayOutputStream bos1 = new ByteArrayOutputStream();
		ByteArrayOutputStream bos2 = new ByteArrayOutputStream();
		ByteArrayOutputStream bos3 = new ByteArrayOutputStream();
		ByteArrayOutputStream bos4 = new ByteArrayOutputStream();

		batch1.writeMessages(bos1);
		batch2.writeMessages(bos2);
		batch3.writeMessages(bos3);
		batch4.writeMessages(bos4);

		BatchStreamsManager batchStreamsManager = new BatchStreamsManager(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(period, 5));

		batchStreamsManager.registerInputStream(new ByteArrayInputStream(bos1.toByteArray()));
		batchStreamsManager.registerInputStream(new ByteArrayInputStream(bos2.toByteArray()));
		batchStreamsManager.registerInputStream(new ByteArrayInputStream(bos3.toByteArray()));
		batchStreamsManager.registerInputStream(new ByteArrayInputStream(bos4.toByteArray()));

		ByteArrayOutputStream toStore = new ByteArrayOutputStream();
		batchStreamsManager.writeMessages(toStore);

		ByteArrayInputStream storeInputStream = new ByteArrayInputStream(toStore.toByteArray());

		for (int i = 0; i < 9; i++)
		{
			Message message = new Message();
			ProtostuffIOUtil.mergeDelimitedFrom(storeInputStream, message, schema);
			assertEquals("testdata" + i, new String(message.getPayload()));
		}

		batch1.clear();
		batch2.clear();
		batch3.clear();
		batch4.clear();
	}
}
