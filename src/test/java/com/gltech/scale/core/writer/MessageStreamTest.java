package com.gltech.scale.core.writer;

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
import static junit.framework.Assert.assertNull;

public class MessageStreamTest
{
	@Test
	public void nextRecordTestMemory() throws Exception
	{
		ChannelMetaData channelMetaData = new ChannelMetaData("1", ChannelMetaData.TTL_DAY, true);
		Batch batch = new BatchMemory(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(DateTime.now(), 5));
		nextTest(batch);
	}

	@Test
	public void nextRecordTestFile() throws Exception
	{
		ChannelMetaData channelMetaData = new ChannelMetaData("1", ChannelMetaData.TTL_DAY, true);
		Batch batch = new BatchNIOFile(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(DateTime.now(), 5));
		nextTest(batch);
	}

	private void nextTest(Batch batch) throws Exception
	{
		ModelIO modelIO = new ModelIO();

		batch.addMessage(modelIO.toBytes(new Message(MediaType.APPLICATION_JSON_TYPE, null, "testdata".getBytes())));
		Thread.sleep(5);
		batch.addMessage(modelIO.toBytes(new Message(MediaType.APPLICATION_JSON_TYPE, null, "testdata2".getBytes())));

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		batch.writeMessages(bos);

		MessageStream messageStream = new MessageInputStream("test", new ByteArrayInputStream(bos.toByteArray()));

		assertEquals("testdata", new String(messageStream.getCurrentMessage().getPayload()));
		messageStream.nextRecord();
		assertEquals("testdata2", new String(messageStream.getCurrentMessage().getPayload()));
		messageStream.nextRecord();
		assertNull(messageStream.getCurrentMessage());

		batch.clear();
	}
}
