package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.model.BatchMetaData;
import com.gltech.scale.core.model.ChannelMetaData;
import com.google.common.base.Throwables;
import com.google.protobuf.CodedOutputStream;
import org.joda.time.DateTime;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class BatchMemory implements Batch
{
	private DateTime nearestPeriodCeiling;
	private DateTime firstMessageTime;
	private DateTime lastMessageTime = DateTime.now();
	private LinkedBlockingQueue<byte[]> messages = new LinkedBlockingQueue<>();
	private AtomicLong bytes = new AtomicLong(0);
	private ChannelMetaData channelMetaData;

	public BatchMemory(ChannelMetaData channelMetaData, DateTime nearestPeriodCeiling)
	{
		this.channelMetaData = channelMetaData;
		this.nearestPeriodCeiling = nearestPeriodCeiling;
	}

	@Override
	public void addMessage(byte[] message)
	{
		messages.add(message);
		lastMessageTime = DateTime.now();
		bytes.addAndGet(message.length);

		if (firstMessageTime == null)
		{
			firstMessageTime = DateTime.now();
		}
	}

	@Override
	public long writeMessages(OutputStream outputStream)
	{
		CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(outputStream);

		try
		{
			for (byte[] bytes : messages)
			{
				codedOutputStream.writeRawVarint32(bytes.length);
				codedOutputStream.writeRawBytes(bytes);

			}
			codedOutputStream.flush();
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}

		return messages.size();
	}

	@Override
	public void clear()
	{
		// No need to do anything for this implementation.  GC will take care of it.
	}

	@Override
	public ChannelMetaData getChannelMetaData()
	{
		return channelMetaData;
	}

	@Override
	public long getMessages()
	{
		return messages.size();
	}

	@Override
	public long getBytes()
	{
		return bytes.get();
	}

	@Override
	public DateTime getLastMessageTime()
	{
		return lastMessageTime;
	}

	@Override
	public DateTime getFirstMessageTime()
	{
		return firstMessageTime;
	}

	@Override
	public BatchMetaData getMetaData()
	{
		return new BatchMetaData(nearestPeriodCeiling, messages.size(), bytes.get(), channelMetaData);
	}

	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		BatchMemory that = (BatchMemory) o;

		if (channelMetaData != null ? !channelMetaData.equals(that.channelMetaData) : that.channelMetaData != null)
			return false;
		if (nearestPeriodCeiling != null ? !nearestPeriodCeiling.equals(that.nearestPeriodCeiling) : that.nearestPeriodCeiling != null)
			return false;

		return true;
	}

	public int hashCode()
	{
		int result = nearestPeriodCeiling != null ? nearestPeriodCeiling.hashCode() : 0;
		result = 31 * result + (channelMetaData != null ? channelMetaData.hashCode() : 0);
		return result;
	}
}
