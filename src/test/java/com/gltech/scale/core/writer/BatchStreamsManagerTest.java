package com.gltech.scale.core.writer;

import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.core.model.Message;
import com.gltech.scale.core.model.Batch;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.util.ModelIO;
import com.google.protobuf.CodedOutputStream;
import org.joda.time.DateTime;
import org.junit.Test;

import javax.ws.rs.core.MediaType;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static junit.framework.Assert.assertEquals;

public class BatchStreamsManagerTest
{
	@Test
	public void nextMultiStreamTest() throws Exception
	{
		ModelIO modelIO = new ModelIO();
		ChannelMetaData channelMetaData = new ChannelMetaData("test", ChannelMetaData.TTL_DAY, false);
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

		DateTime period = DateTime.now();

		Batch batch1 = new Batch(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(period, 5));
		Batch batch2 = new Batch(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(period, 5));
		Batch batch3 = new Batch(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(period, 5));
		Batch batch4 = new Batch(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(period, 5));

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

		CodedOutputStream cos1 = CodedOutputStream.newInstance(bos1);
		for (byte[] bytes : batch1.getMessages())
		{
			cos1.writeRawVarint32(bytes.length);
			cos1.writeRawBytes(bytes);
		}
		cos1.flush();

		CodedOutputStream cos2 = CodedOutputStream.newInstance(bos2);
		for (byte[] bytes : batch2.getMessages())
		{
			cos2.writeRawVarint32(bytes.length);
			cos2.writeRawBytes(bytes);
		}
		cos2.flush();

		CodedOutputStream cos3 = CodedOutputStream.newInstance(bos3);
		for (byte[] bytes : batch3.getMessages())
		{
			cos3.writeRawVarint32(bytes.length);
			cos3.writeRawBytes(bytes);
		}
		cos3.flush();

		CodedOutputStream cos4 = CodedOutputStream.newInstance(bos4);
		for (byte[] bytes : batch4.getMessages())
		{
			cos4.writeRawVarint32(bytes.length);
			cos4.writeRawBytes(bytes);
		}
		cos4.flush();

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
	}
}
