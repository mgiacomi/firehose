package com.gltech.scale.core.model;

import com.dyuproject.protostuff.Tag;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BatchMetaData implements Comparable<BatchMetaData>
{
	@Tag(1)
	private DateTime nearestPeriodCeiling;
	@Tag(2)
	private ChannelMetaData channelMetaData;
	@Tag(3)
	private long eventsAdded;
	@Tag(4)
	private long bytes;

	public BatchMetaData()
	{
		nearestPeriodCeiling = null;
		channelMetaData = null;
		eventsAdded = -1;
		bytes = -1;
	}

	public BatchMetaData(DateTime nearestPeriodCeiling, long eventsAdded, long bytes, ChannelMetaData channelMetaData)
	{
		this.nearestPeriodCeiling = nearestPeriodCeiling;
		this.eventsAdded = eventsAdded;
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

	public long getEventsAdded()
	{
		return eventsAdded;
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
		if (eventsAdded != that.eventsAdded) return false;
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
		result = 31 * result + (int) (eventsAdded ^ (eventsAdded >>> 32));
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
				", eventsAdded=" + eventsAdded +
				", bucketMetaData=" + channelMetaData +
				'}' +
				channelMetaData.toString();
	}
}
