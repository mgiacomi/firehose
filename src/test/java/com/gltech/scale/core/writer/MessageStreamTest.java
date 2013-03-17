package com.gltech.scale.core.writer;

import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.core.model.Message;
import com.gltech.scale.core.model.Batch;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.util.ModelIO;
import com.gltech.scale.util.StreamDelimiter;
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
		StreamDelimiter streamDelimiter = new StreamDelimiter();
		ChannelMetaData channelMetaData = new ChannelMetaData("1", ChannelMetaData.TTL_DAY, true);
		ModelIO modelIO = new ModelIO();

		Batch batch = new Batch(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(DateTime.now(), 5));
		batch.addMessage(modelIO.toBytes(new Message(MediaType.APPLICATION_JSON_TYPE, "testdata".getBytes())));
		Thread.sleep(5);
		batch.addMessage(modelIO.toBytes(new Message(MediaType.APPLICATION_JSON_TYPE, "testdata2".getBytes())));

		assertEquals("testdata", new String(modelIO.toMessage(batch.getMessages().get(0)).getPayload()));
		assertEquals("testdata2", new String(modelIO.toMessage(batch.getMessages().get(1)).getPayload()));

		ByteArrayOutputStream bos = new ByteArrayOutputStream();

		for(byte[] bytes : batch.getMessages())
		{
			streamDelimiter.write(bos, bytes);
		}

		MessageStream messageStream = new MessageInputStream("test", new ByteArrayInputStream(bos.toByteArray()));

		assertEquals("testdata", new String(messageStream.getCurrentMessage().getPayload()));
		messageStream.nextRecord();
		assertEquals("testdata2", new String(messageStream.getCurrentMessage().getPayload()));
		messageStream.nextRecord();
		assertNull(messageStream.getCurrentMessage());
	}
}
