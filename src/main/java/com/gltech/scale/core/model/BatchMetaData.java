package com.gltech.scale.core.model;

import com.dyuproject.protostuff.Tag;
import org.joda.time.DateTime;

public class BatchMetaData implements Comparable<BatchMetaData>
{
	@Tag(1)
	private DateTime nearestPeriodCeiling;
	@Tag(2)
	private ChannelMetaData channelMetaData;
	@Tag(3)
	private long messagesAdded;
	@Tag(4)
	private long bytes;

	public BatchMetaData()
	{
		nearestPeriodCeiling = null;
		channelMetaData = null;
		messagesAdded = -1;
		bytes = -1;
	}

	public BatchMetaData(DateTime nearestPeriodCeiling, long messagesAdded, long bytes, ChannelMetaData channelMetaData)
	{
		this.nearestPeriodCeiling = nearestPeriodCeiling;
		this.messagesAdded = messagesAdded;
		this.bytes = bytes;
		this.channelMetaData = channelMetaData;
	}

	public DateTime getNearestPeriodCeiling()
	{
		return nearestPeriodCeiling;
	}

	public ChannelMetaData getChannelMetaData()
	{
		return channelMetaData;
	}

	public long getMessagesAdded()
	{
		return messagesAdded;
	}

	public long getBytes()
	{
		return bytes;
	}

	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		BatchMetaData that = (BatchMetaData) o;

		if (bytes != that.bytes) return false;
		if (messagesAdded != that.messagesAdded) return false;
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
		result = 31 * result + (int) (messagesAdded ^ (messagesAdded >>> 32));
		result = 31 * result + (int) (bytes ^ (bytes >>> 32));
		return result;
	}

	@Override
	public int compareTo(BatchMetaData that)
	{
		return this.nearestPeriodCeiling.compareTo(that.getNearestPeriodCeiling());
	}

	public String toString()
	{
		return "BatchMetaData{" +
				"nearestPeriodCeiling=" + nearestPeriodCeiling +
				", bytes=" + bytes +
				", messagesAdded=" + messagesAdded +
				", bucketMetaData=" + channelMetaData +
				'}' +
				channelMetaData.toString();
	}
}
