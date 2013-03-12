package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.cluster.ClusterService;
import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.core.model.Message;
import com.gltech.scale.core.model.Batch;
import com.gltech.scale.core.model.ChannelMetaData;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ChannelImpl implements Channel
{
	private static final Logger logger = LoggerFactory.getLogger("com.lokiscale.rope.RopeImpl");
	private final ConcurrentMap<DateTime, Batch> timeBuckets = new ConcurrentHashMap<>();
	private final ConcurrentMap<DateTime, Batch> backupTimeBuckets = new ConcurrentHashMap<>();
	private final ChannelMetaData channelMetaData;
	private final ClusterService clusterService;
	private final TimePeriodUtils timePeriodUtils;

	public ChannelImpl(ChannelMetaData channelMetaData, ClusterService clusterService, TimePeriodUtils timePeriodUtils)
	{
		this.channelMetaData = channelMetaData;
		this.clusterService = clusterService;
		this.timePeriodUtils = timePeriodUtils;
	}

	public void addEvent(Message message)
	{
		DateTime nearestPeriodCeiling = timePeriodUtils.nearestPeriodCeiling(message.getReceived_at());
		Batch batch = timeBuckets.get(nearestPeriodCeiling);

		if (batch == null)
		{
			// Register TimeBucket for collection
			clusterService.addTimeBucket(channelMetaData, nearestPeriodCeiling);

			Batch newBatch = new Batch(channelMetaData, nearestPeriodCeiling);
			batch = timeBuckets.putIfAbsent(nearestPeriodCeiling, newBatch);
			if (batch == null)
			{
				batch = newBatch;
				logger.info("Creating TimeBucket {customer=" + channelMetaData.getCustomer() + "|" + channelMetaData.getBucket() + "|" + nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")));
			}
		}

		// Using the good old .isDebugEnabled() so we don't create extra String objects (on json payloads) if debug is not enabled.
		if (logger.isDebugEnabled())
		{
			if (channelMetaData.getMediaType().equals(MediaType.APPLICATION_JSON_TYPE))
			{
				if (message.isStored())
				{
					logger.trace("Add event customer={} bucket={} key={} payload=stored", channelMetaData.getCustomer(), channelMetaData.getBucket(), nearestPeriodCeiling);
				}
				else
				{
					logger.trace("Add event customer={} bucket={} key={} payload={}", channelMetaData.getCustomer(), channelMetaData.getBucket(), nearestPeriodCeiling, new String(message.getPayload()));
				}
			}
			else
			{
				logger.trace("Add event customer={} bucket={} key={} payload_size={}", channelMetaData.getCustomer(), channelMetaData.getBucket(), nearestPeriodCeiling, message.getPayload().length);
			}
		}

		batch.addEvent(message);
	}

	public void addBackupEvent(Message message)
	{
		DateTime nearestPeriodCeiling = timePeriodUtils.nearestPeriodCeiling(message.getReceived_at());
		Batch batch = backupTimeBuckets.get(nearestPeriodCeiling);

		if (batch == null)
		{
			logger.info("Creating Backup TimeBucket " + channelMetaData.getCustomer() + "|" + channelMetaData.getBucket() + "|" + nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")));

			// A coordinationId will be assigned to passed in timeBucketMetaData.
			clusterService.addTimeBucket(channelMetaData, nearestPeriodCeiling);

			Batch newBatch = new Batch(channelMetaData, nearestPeriodCeiling);
			batch = backupTimeBuckets.putIfAbsent(nearestPeriodCeiling, newBatch);
			if (batch == null)
			{
				batch = newBatch;
			}
		}

		// Using the good old .isDebugEnabled() so we don't create extra String objects (on json payloads) if debug is not enabled.
		if (logger.isDebugEnabled())
		{
			if (channelMetaData.getMediaType().equals(MediaType.APPLICATION_JSON_TYPE))
			{
				logger.trace("Add backup event customer={} bucket={} key={} payload={}", channelMetaData.getCustomer(), channelMetaData.getBucket(), nearestPeriodCeiling, new String(message.getPayload()));
			}
			else
			{
				logger.trace("Add backup event customer={} bucket={} key={} payload_size={}", channelMetaData.getCustomer(), channelMetaData.getBucket(), nearestPeriodCeiling, message.getPayload().length);
			}
		}

		batch.addEvent(message);
	}

	public ChannelMetaData getChannelMetaData()
	{
		return channelMetaData;
	}

	public Collection<Batch> getTimeBuckets()
	{
		return Collections.unmodifiableCollection(timeBuckets.values());
	}

	public Collection<Batch> getBackupTimeBuckets()
	{
		return Collections.unmodifiableCollection(backupTimeBuckets.values());
	}

	public Batch getTimeBucket(DateTime nearestPeriodCeiling)
	{
		return timeBuckets.get(nearestPeriodCeiling);
	}

	public Batch getBackupTimeBucket(DateTime nearestPeriodCeiling)
	{
		return backupTimeBuckets.get(nearestPeriodCeiling);
	}

	public void clear(DateTime nearestPeriodCeiling)
	{
		timeBuckets.remove(nearestPeriodCeiling);
		logger.info("Cleared TimeBucket " + channelMetaData.getCustomer() + "|" + channelMetaData.getBucket() + "|" + nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")));
	}

	public void clearBackup(DateTime nearestPeriodCeiling)
	{
		backupTimeBuckets.remove(nearestPeriodCeiling);
		logger.info("Cleared backup TimeBuckets " + channelMetaData.getCustomer() + "|" + channelMetaData.getBucket() + "|" + nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")));

	}
}
