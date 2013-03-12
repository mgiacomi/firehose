package com.gltech.scale.core.rope;

import com.gltech.scale.core.model.Message;
import com.google.inject.Inject;
import com.gltech.scale.core.cluster.ClusterService;
import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.core.storage.BucketMetaData;
import com.gltech.scale.core.storage.BucketMetaDataCache;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RopeManagerImpl implements RopeManager
{
	private static final Logger logger = LoggerFactory.getLogger("com.lokiscale.rope.RopeManagerSingleAndDoubleWrite");
	private ConcurrentMap<BucketMetaData, Rope> ropes = new ConcurrentHashMap<>();
	private BucketMetaDataCache bucketMetaDataCache;
	private ClusterService clusterService;
	private TimePeriodUtils timePeriodUtils;

	@Inject
	public RopeManagerImpl(BucketMetaDataCache bucketMetaDataCache, ClusterService clusterService, TimePeriodUtils timePeriodUtils)
	{
		this.bucketMetaDataCache = bucketMetaDataCache;
		this.clusterService = clusterService;
		this.timePeriodUtils = timePeriodUtils;

		// Register the rope manager with the coordination service, so that collectors can find us
		clusterService.getRegistrationService().registerAsRopeManager();
	}

	@Override
	public void addEvent(Message message)
	{
		BucketMetaData bucketMetaData = bucketMetaDataCache.getBucketMetaData(message.getCustomer(), message.getBucket(), true);
		Rope rope = ropes.get(bucketMetaData);

		if (rope == null)
		{
			Rope newRope = new RopeStats(new RopeImpl(bucketMetaData, clusterService, timePeriodUtils));
			rope = ropes.putIfAbsent(bucketMetaData, newRope);
			if (rope == null)
			{
				rope = newRope;
				logger.info("Creating Rope {customer=" + message.getCustomer() + ", bucket=" + message.getBucket() + "}");
			}
		}

		rope.addEvent(message);
	}

	@Override
	public void addBackupEvent(Message message)
	{
		BucketMetaData bucketMetaData = bucketMetaDataCache.getBucketMetaData(message.getCustomer(), message.getBucket(), true);

		Rope rope = ropes.get(bucketMetaData);

		if (rope == null)
		{
			logger.info("Creating Rope {customer=" + message.getCustomer() + ", bucket=" + message.getBucket() + "}");
			Rope newRope = new RopeStats(new RopeImpl(bucketMetaData, clusterService, timePeriodUtils));
			rope = ropes.putIfAbsent(bucketMetaData, newRope);
			if (rope == null)
			{
				rope = newRope;
			}
		}

		rope.addBackupEvent(message);
	}

	@Override
	public void clear(String customer, String bucket, DateTime dateTime)
	{
		BucketMetaData bucketMetaData = bucketMetaDataCache.getBucketMetaData(customer, bucket, false);
		Rope rope = ropes.get(bucketMetaData);

		if (rope != null)
		{
			rope.clear(timePeriodUtils.nearestPeriodCeiling(dateTime));
			if (bucketMetaData.isDoubleWrite())
			{
				rope.clearBackup(timePeriodUtils.nearestPeriodCeiling(dateTime));
			}
		}
	}

	@Override
	public List<TimeBucket> getActiveTimeBuckets()
	{
		List<TimeBucket> activeTimeBuckets = new ArrayList<>();

		for (Rope rope : ropes.values())
		{
			for (TimeBucket timeBucket : rope.getTimeBuckets())
			{
				activeTimeBuckets.add(timeBucket);
			}
		}

		return Collections.unmodifiableList(activeTimeBuckets);
	}

	@Override
	public List<TimeBucket> getActiveBackupTimeBuckets()
	{
		List<TimeBucket> activeBackupTimeBuckets = new ArrayList<>();

		for (Rope rope : ropes.values())
		{
			for (TimeBucket timeBucket : rope.getBackupTimeBuckets())
			{
				activeBackupTimeBuckets.add(timeBucket);
			}
		}

		return Collections.unmodifiableList(activeBackupTimeBuckets);
	}

	@Override
	public long writeTimeBucketEvents(OutputStream outputStream, String customer, String bucket, DateTime dateTime)
	{
		BucketMetaData bucketMetaData = bucketMetaDataCache.getBucketMetaData(customer, bucket, false);

		if (bucketMetaData != null)
		{
			Rope rope = ropes.get(bucketMetaData);

			if (rope != null)
			{
				TimeBucket timeBucket = ropes.get(bucketMetaData).getTimeBucket(timePeriodUtils.nearestPeriodCeiling(dateTime));

				if (timeBucket != null)
				{
					return timeBucketEventsToStream(outputStream, timeBucket);
				}
			}
		}

		return 0;
	}

	@Override
	public long writeBackupTimeBucketEvents(OutputStream outputStream, String customer, String bucket, DateTime dateTime)
	{
		BucketMetaData bucketMetaData = bucketMetaDataCache.getBucketMetaData(customer, bucket, false);

		if (bucketMetaData != null)
		{
			Rope rope = ropes.get(bucketMetaData);

			if (rope != null)
			{
				TimeBucket timeBucket = rope.getBackupTimeBucket(timePeriodUtils.nearestPeriodCeiling(dateTime));

				if (timeBucket != null)
				{
					return timeBucketEventsToStream(outputStream, timeBucket);
				}
			}
		}

		return 0;
	}

	private long timeBucketEventsToStream(OutputStream outputStream, TimeBucket timeBucket)
	{
		timeBucket.eventsToJson(outputStream);
		return timeBucket.getEvents().size();
	}

	@Override
	public TimeBucketMetaData getTimeBucketMetaData(String customer, String bucket, DateTime dateTime)
	{
		BucketMetaData bucketMetaData = bucketMetaDataCache.getBucketMetaData(customer, bucket, false);
		return ropes.get(bucketMetaData).getTimeBucket(timePeriodUtils.nearestPeriodCeiling(dateTime)).getMetaData();
	}

	@Override
	public TimeBucketMetaData getBackupTimeBucketMetaData(String customer, String bucket, DateTime dateTime)
	{
		BucketMetaData bucketMetaData = bucketMetaDataCache.getBucketMetaData(customer, bucket, false);
		return ropes.get(bucketMetaData).getBackupTimeBucket(timePeriodUtils.nearestPeriodCeiling(dateTime)).getMetaData();
	}

	@Override
	public void shutdown()
	{
		clusterService.getRegistrationService().unRegisterAsRopeManager();
	}
}
