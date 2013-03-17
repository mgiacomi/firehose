package com.gltech.scale.core.model;

import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class Batch
{
	private DateTime nearestPeriodCeiling;
	private DateTime firstMessageTime;
	private DateTime lastMessageTime = DateTime.now();
	private LinkedBlockingQueue<byte[]> messages = new LinkedBlockingQueue<>();
	private AtomicLong bytes = new AtomicLong(0);
	private ChannelMetaData channelMetaData;

	public Batch(ChannelMetaData channelMetaData, DateTime nearestPeriodCeiling)
	{
		this.channelMetaData = channelMetaData;
		this.nearestPeriodCeiling = nearestPeriodCeiling;
	}

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

	public ChannelMetaData getChannelMetaData()
	{
		return channelMetaData;
	}

	public List<byte[]> getMessages()
	{
		return Collections.unmodifiableList(new ArrayList<>(messages));
	}

	public long getBytes()
	{
		return bytes.get();
	}

	public DateTime getLastMessageTime()
	{
		return lastMessageTime;
	}

	public DateTime getFirstMessageTime()
	{
		return firstMessageTime;
	}

	public BatchMetaData getMetaData()
	{
		return new BatchMetaData(nearestPeriodCeiling, messages.size(), bytes.get(), channelMetaData);
	}

	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Batch that = (Batch) o;

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
