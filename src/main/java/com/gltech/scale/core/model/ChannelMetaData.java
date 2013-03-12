package com.gltech.scale.core.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gltech.scale.core.storage.BucketMetaDataException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.io.IOException;

public class ChannelMetaData
{
	private static final Logger logger = LoggerFactory.getLogger(ChannelMetaData.class);
	private static final ObjectMapper mapper = new ObjectMapper();

	public enum BucketType
	{
		eventset,
		bytes
	}

	/**
	 * Short Term - two weeks
	 * Medium Term - 3 months
	 * large Term - 1 year
	 */
	public enum LifeTime
	{
		small,
		medium,
		large
	}

	public enum Redundancy
	{
		singlewrite,
		doublewritesync
	}

	private String customer;
	private String bucket;

	//desired redundancy

	private BucketType bucketType;
	private MediaType mediaType;
	private LifeTime lifeTime;
	private int periodSeconds;
	private Redundancy redundancy;

	public ChannelMetaData(String customer, String bucket, BucketType bucketType, int periodSeconds, MediaType mediaType, LifeTime lifeTime, Redundancy redundancy)
	{
		this.customer = customer;
		this.bucket = bucket;
		this.bucketType = bucketType;
		this.periodSeconds = periodSeconds;
		this.mediaType = mediaType;
		this.lifeTime = lifeTime;
		this.redundancy = redundancy;
		validatePeriodSeconds();
	}

	public ChannelMetaData(String json)
	{
		this(null, null, json);
		try
		{
			JsonNode rootNode = mapper.readTree(json);
			this.customer = rootNode.path("customer").asText();
			this.bucket = rootNode.path("bucket").asText();
		}
		catch (IOException e)
		{
			logger.warn("unable to parse json {}", json, e);
			throw new BucketMetaDataException("unable to parse json " + json, e);
		}
	}

	public ChannelMetaData(String customer, String bucket, String json)
	{
		this.customer = customer;
		this.bucket = bucket;
		logger.trace("customer={} bucket={} json={}", customer, bucket, json);
		//todo - gfm - 9/21/12 - return if json is empty
		try
		{
			JsonNode rootNode = mapper.readTree(json);
			bucketType = BucketType.valueOf(rootNode.path("bucketType").asText().toLowerCase());
			switch (bucketType)
			{
				case eventset:
					periodSeconds = rootNode.path("periodSeconds").asInt(60);
					lifeTime = LifeTime.small;
					redundancy = Redundancy.valueOf(rootNode.path("redundancy").asText().toLowerCase());
					validatePeriodSeconds();
					break;
				case bytes:
					lifeTime = LifeTime.valueOf(rootNode.path("lifeTime").asText().toLowerCase());
					break;
			}
			mediaType = MediaType.valueOf(rootNode.path("mediaType").asText());

		}
		catch (IllegalArgumentException | IOException e)
		{
			logger.warn("unable to parse json {}", json, e);
			throw new BucketMetaDataException("unable to parse json " + json, e);
		}

	}

	public JsonNode toJson()
	{
		ObjectNode objectNode = mapper.createObjectNode();
		objectNode.put("customer", customer);
		objectNode.put("bucket", bucket);
		objectNode.put("bucketType", bucketType.toString());
		objectNode.put("mediaType", mediaType.toString());
		objectNode.put("lifeTime", lifeTime.toString());

		if (bucketType.equals(BucketType.eventset))
		{
			objectNode.put("periodSeconds", periodSeconds);
			objectNode.put("redundancy", redundancy.toString());
		}
		return objectNode;
	}

	public String getCustomer()
	{
		return customer;
	}

	public int getPeriodSeconds()
	{
		return periodSeconds;
	}

	public String getBucket()
	{
		return bucket;
	}

	public MediaType getMediaType()
	{
		return mediaType;
	}

	public BucketType getBucketType()
	{
		return bucketType;
	}

	public Redundancy getRedundancy()
	{
		return redundancy;
	}

	public boolean isDoubleWrite()
	{
		return Redundancy.doublewritesync.equals(redundancy);
	}

	public String toString()
	{
		return "BucketMetaData{" +
				"customer='" + customer + '\'' +
				", bucket='" + bucket + "\' " +
				toJson().toString() +
				'}';
	}

	public DateTime nearestPeriodCeiling(DateTime dateTime)
	{
		double min = dateTime.getMinuteOfHour();
		double sec = dateTime.getSecondOfMinute();

		dateTime = dateTime.withMillisOfSecond(0);

		// Bucket by minutes
		if (periodSeconds >= 60)
		{
			int newMin = ((int) Math.ceil((min * 60 + sec) / periodSeconds)) * periodSeconds / 60;

			if (newMin == 60)
			{
				dateTime = dateTime.plusHours(1);
				newMin = 0;
			}

			dateTime = dateTime.withMinuteOfHour(newMin).withSecondOfMinute(0);
		}
		// Bucket by seconds
		else
		{
			int newSec = ((int) Math.ceil(sec / periodSeconds)) * periodSeconds;

			if (newSec == 60)
			{
				dateTime = dateTime.plusMinutes(1);
				newSec = 0;
			}

			dateTime = dateTime.withSecondOfMinute(newSec);
		}

		return dateTime;
	}

	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ChannelMetaData that = (ChannelMetaData) o;

		if (bucket != null ? !bucket.equals(that.bucket) : that.bucket != null) return false;
		if (customer != null ? !customer.equals(that.customer) : that.customer != null) return false;

		return true;
	}

	public int hashCode()
	{
		int result = customer != null ? customer.hashCode() : 0;
		result = 31 * result + (bucket != null ? bucket.hashCode() : 0);
		return result;
	}

	// Use modulo to make sure the interval is divisible by an hour.
	// Otherwise the buckets are not going to make sense over time.
	void validatePeriodSeconds()
	{
		if (3600 % periodSeconds != 0)
		{
			String error = "periodSeconds must be divisible by an hour. Invalid value: " + periodSeconds;
			logger.warn(error);
			throw new IllegalArgumentException(error);
		}
	}

	public LifeTime getLifeTime()
	{
		return lifeTime;
	}
}
