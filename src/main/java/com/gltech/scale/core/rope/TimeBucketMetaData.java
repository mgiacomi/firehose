package com.gltech.scale.core.rope;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gltech.scale.core.storage.BucketMetaData;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TimeBucketMetaData implements Comparable<TimeBucketMetaData>
{
	private static final Logger logger = LoggerFactory.getLogger("com.lokiscale.rope.TimeBucketMetaData");
	private static final ObjectMapper mapper = new ObjectMapper();

	private DateTime nearestPeriodCeiling;
	private BucketMetaData bucketMetaData;
	private long eventsAdded;
	private long bytes;

	public TimeBucketMetaData(DateTime nearestPeriodCeiling, long eventsAdded, long bytes, BucketMetaData bucketMetaData)
	{
		this.nearestPeriodCeiling = nearestPeriodCeiling;
		this.eventsAdded = eventsAdded;
		this.bytes = bytes;
		this.bucketMetaData = bucketMetaData;
	}

	public TimeBucketMetaData(String json)
	{
		try
		{
			JsonNode rootNode = mapper.readTree(json);
			this.nearestPeriodCeiling = new DateTime(rootNode.path("nearestPeriodCeiling").asText());
			this.bytes = rootNode.path("bytes").asLong();
			this.eventsAdded = rootNode.path("eventsAdded").asLong();
			this.bucketMetaData = new BucketMetaData(rootNode.path("bucketMetaData").asText());
		}
		catch (IOException e)
		{
			logger.warn("unable to parse json {}", json, e);
			throw new RuntimeException("unable to parse json " + json, e);
		}
	}

	public JsonNode toJson()
	{
		ObjectNode objectNode = mapper.createObjectNode();
		objectNode.put("nearestPeriodCeiling", nearestPeriodCeiling.toString());
		objectNode.put("bytes", bytes);
		objectNode.put("eventsAdded", eventsAdded);
		objectNode.put("bucketMetaData", bucketMetaData.toJson().toString());

		return objectNode;
	}

	public DateTime getNearestPeriodCeiling()
	{
		return nearestPeriodCeiling;
	}

	public BucketMetaData getBucketMetaData()
	{
		return bucketMetaData;
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

		TimeBucketMetaData that = (TimeBucketMetaData) o;

		if (bytes != that.bytes) return false;
		if (eventsAdded != that.eventsAdded) return false;
		if (bucketMetaData != null ? !bucketMetaData.equals(that.bucketMetaData) : that.bucketMetaData != null)
			return false;
		if (nearestPeriodCeiling != null ? !nearestPeriodCeiling.equals(that.nearestPeriodCeiling) : that.nearestPeriodCeiling != null)
			return false;

		return true;
	}

	public int hashCode()
	{
		int result = nearestPeriodCeiling != null ? nearestPeriodCeiling.hashCode() : 0;
		result = 31 * result + (bucketMetaData != null ? bucketMetaData.hashCode() : 0);
		result = 31 * result + (int) (eventsAdded ^ (eventsAdded >>> 32));
		result = 31 * result + (int) (bytes ^ (bytes >>> 32));
		return result;
	}

	@Override
	public int compareTo(TimeBucketMetaData that)
	{
		return this.nearestPeriodCeiling.compareTo(that.getNearestPeriodCeiling());
	}

	public String toString()
	{
		return "TimeBucketMetaData{" +
				"nearestPeriodCeiling=" + nearestPeriodCeiling +
				", bytes=" + bytes +
				", eventsAdded=" + eventsAdded +
				", bucketMetaData=" + bucketMetaData +
				'}' +
				bucketMetaData.toString();
	}
}
