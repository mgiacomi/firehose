package com.gltech.scale.core.rope;

import com.gltech.scale.core.coordination.CoordinationService;
import com.gltech.scale.core.coordination.TimePeriodUtils;
import com.gltech.scale.core.event.EventPayload;
import com.gltech.scale.core.storage.BucketMetaData;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RopeImpl implements Rope
{
	private static final Logger logger = LoggerFactory.getLogger("com.lokiscale.rope.RopeImpl");
	private final ConcurrentMap<DateTime, TimeBucket> timeBuckets = new ConcurrentHashMap<>();
	private final ConcurrentMap<DateTime, TimeBucket> backupTimeBuckets = new ConcurrentHashMap<>();
	private final BucketMetaData bucketMetaData;
	private final CoordinationService coordinationService;
	private final TimePeriodUtils timePeriodUtils;

	public RopeImpl(BucketMetaData bucketMetaData, CoordinationService coordinationService, TimePeriodUtils timePeriodUtils)
	{
		this.bucketMetaData = bucketMetaData;
		this.coordinationService = coordinationService;
		this.timePeriodUtils = timePeriodUtils;
	}

	public void addEvent(EventPayload eventPayload)
	{
		DateTime nearestPeriodCeiling = timePeriodUtils.nearestPeriodCeiling(eventPayload.getReceived_at());
		TimeBucket timeBucket = timeBuckets.get(nearestPeriodCeiling);

		if (timeBucket == null)
		{
			// Register TimeBucket for collection
			coordinationService.addTimeBucket(bucketMetaData, nearestPeriodCeiling);

			TimeBucket newTimeBucket = new TimeBucket(bucketMetaData, nearestPeriodCeiling);
			timeBucket = timeBuckets.putIfAbsent(nearestPeriodCeiling, newTimeBucket);
			if (timeBucket == null)
			{
				timeBucket = newTimeBucket;
				logger.info("Creating TimeBucket {customer=" + bucketMetaData.getCustomer() + "|" + bucketMetaData.getBucket() + "|" + nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")));
			}
		}

		// Using the good old .isDebugEnabled() so we don't create extra String objects (on json payloads) if debug is not enabled.
		if (logger.isDebugEnabled())
		{
			if (bucketMetaData.getMediaType().equals(MediaType.APPLICATION_JSON_TYPE))
			{
				if (eventPayload.isStored())
				{
					logger.trace("Add event customer={} bucket={} key={} payload=stored", bucketMetaData.getCustomer(), bucketMetaData.getBucket(), nearestPeriodCeiling);
				}
				else
				{
					logger.trace("Add event customer={} bucket={} key={} payload={}", bucketMetaData.getCustomer(), bucketMetaData.getBucket(), nearestPeriodCeiling, new String(eventPayload.getPayload()));
				}
			}
			else
			{
				logger.trace("Add event customer={} bucket={} key={} payload_size={}", bucketMetaData.getCustomer(), bucketMetaData.getBucket(), nearestPeriodCeiling, eventPayload.getPayload().length);
			}
		}

		timeBucket.addEvent(eventPayload);
	}

	public void addBackupEvent(EventPayload eventPayload)
	{
		DateTime nearestPeriodCeiling = timePeriodUtils.nearestPeriodCeiling(eventPayload.getReceived_at());
		TimeBucket timeBucket = backupTimeBuckets.get(nearestPeriodCeiling);

		if (timeBucket == null)
		{
			logger.info("Creating Backup TimeBucket " + bucketMetaData.getCustomer() + "|" + bucketMetaData.getBucket() + "|" + nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")));

			// A coordinationId will be assigned to passed in timeBucketMetaData.
			coordinationService.addTimeBucket(bucketMetaData, nearestPeriodCeiling);

			TimeBucket newTimeBucket = new TimeBucket(bucketMetaData, nearestPeriodCeiling);
			timeBucket = backupTimeBuckets.putIfAbsent(nearestPeriodCeiling, newTimeBucket);
			if (timeBucket == null)
			{
				timeBucket = newTimeBucket;
			}
		}

		// Using the good old .isDebugEnabled() so we don't create extra String objects (on json payloads) if debug is not enabled.
		if (logger.isDebugEnabled())
		{
			if (bucketMetaData.getMediaType().equals(MediaType.APPLICATION_JSON_TYPE))
			{
				logger.trace("Add backup event customer={} bucket={} key={} payload={}", bucketMetaData.getCustomer(), bucketMetaData.getBucket(), nearestPeriodCeiling, new String(eventPayload.getPayload()));
			}
			else
			{
				logger.trace("Add backup event customer={} bucket={} key={} payload_size={}", bucketMetaData.getCustomer(), bucketMetaData.getBucket(), nearestPeriodCeiling, eventPayload.getPayload().length);
			}
		}

		timeBucket.addEvent(eventPayload);
	}

	public BucketMetaData getBucketMetaData()
	{
		return bucketMetaData;
	}

	public Collection<TimeBucket> getTimeBuckets()
	{
		return Collections.unmodifiableCollection(timeBuckets.values());
	}

	public Collection<TimeBucket> getBackupTimeBuckets()
	{
		return Collections.unmodifiableCollection(backupTimeBuckets.values());
	}

	public TimeBucket getTimeBucket(DateTime nearestPeriodCeiling)
	{
		return timeBuckets.get(nearestPeriodCeiling);
	}

	public TimeBucket getBackupTimeBucket(DateTime nearestPeriodCeiling)
	{
		return backupTimeBuckets.get(nearestPeriodCeiling);
	}

	public void clear(DateTime nearestPeriodCeiling)
	{
		timeBuckets.remove(nearestPeriodCeiling);
		logger.info("Cleared TimeBucket " + bucketMetaData.getCustomer() + "|" + bucketMetaData.getBucket() + "|" + nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")));
	}

	public void clearBackup(DateTime nearestPeriodCeiling)
	{
		backupTimeBuckets.remove(nearestPeriodCeiling);
		logger.info("Cleared backup TimeBuckets " + bucketMetaData.getCustomer() + "|" + bucketMetaData.getBucket() + "|" + nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")));

	}
}
