package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.model.BatchMetaData;
import com.gltech.scale.core.model.Message;
import com.gltech.scale.core.model.Batch;
import com.google.inject.Inject;
import com.gltech.scale.core.cluster.ClusterService;
import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.core.model.ChannelMetaData;
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

public class AggregatorImpl implements Aggregator
{
	private static final Logger logger = LoggerFactory.getLogger(Aggregator.class);
	private ConcurrentMap<ChannelMetaData, Channel> ropes = new ConcurrentHashMap<>();
	private BucketMetaDataCache bucketMetaDataCache;
	private ClusterService clusterService;
	private TimePeriodUtils timePeriodUtils;

	@Inject
	public AggregatorImpl(BucketMetaDataCache bucketMetaDataCache, ClusterService clusterService, TimePeriodUtils timePeriodUtils)
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
		ChannelMetaData channelMetaData = bucketMetaDataCache.getBucketMetaData(message.getCustomer(), message.getBucket(), true);
		Channel channel = ropes.get(channelMetaData);

		if (channel == null)
		{
			Channel newChannel = new ChannelStats(new ChannelImpl(channelMetaData, clusterService, timePeriodUtils));
			channel = ropes.putIfAbsent(channelMetaData, newChannel);
			if (channel == null)
			{
				channel = newChannel;
				logger.info("Creating Rope {customer=" + message.getCustomer() + ", bucket=" + message.getBucket() + "}");
			}
		}

		channel.addEvent(message);
	}

	@Override
	public void addBackupEvent(Message message)
	{
		ChannelMetaData channelMetaData = bucketMetaDataCache.getBucketMetaData(message.getCustomer(), message.getBucket(), true);

		Channel channel = ropes.get(channelMetaData);

		if (channel == null)
		{
			logger.info("Creating Rope {customer=" + message.getCustomer() + ", bucket=" + message.getBucket() + "}");
			Channel newChannel = new ChannelStats(new ChannelImpl(channelMetaData, clusterService, timePeriodUtils));
			channel = ropes.putIfAbsent(channelMetaData, newChannel);
			if (channel == null)
			{
				channel = newChannel;
			}
		}

		channel.addBackupEvent(message);
	}

	@Override
	public void clear(String customer, String bucket, DateTime dateTime)
	{
		ChannelMetaData channelMetaData = bucketMetaDataCache.getBucketMetaData(customer, bucket, false);
		Channel channel = ropes.get(channelMetaData);

		if (channel != null)
		{
			channel.clear(timePeriodUtils.nearestPeriodCeiling(dateTime));
			if (channelMetaData.isDoubleWrite())
			{
				channel.clearBackup(timePeriodUtils.nearestPeriodCeiling(dateTime));
			}
		}
	}

	@Override
	public List<Batch> getActiveTimeBuckets()
	{
		List<Batch> activeBatches = new ArrayList<>();

		for (Channel channel : ropes.values())
		{
			for (Batch batch : channel.getTimeBuckets())
			{
				activeBatches.add(batch);
			}
		}

		return Collections.unmodifiableList(activeBatches);
	}

	@Override
	public List<Batch> getActiveBackupTimeBuckets()
	{
		List<Batch> activeBackupBatches = new ArrayList<>();

		for (Channel channel : ropes.values())
		{
			for (Batch batch : channel.getBackupTimeBuckets())
			{
				activeBackupBatches.add(batch);
			}
		}

		return Collections.unmodifiableList(activeBackupBatches);
	}

	@Override
	public long writeTimeBucketEvents(OutputStream outputStream, String customer, String bucket, DateTime dateTime)
	{
		ChannelMetaData channelMetaData = bucketMetaDataCache.getBucketMetaData(customer, bucket, false);

		if (channelMetaData != null)
		{
			Channel channel = ropes.get(channelMetaData);

			if (channel != null)
			{
				Batch batch = ropes.get(channelMetaData).getTimeBucket(timePeriodUtils.nearestPeriodCeiling(dateTime));

				if (batch != null)
				{
					return timeBucketEventsToStream(outputStream, batch);
				}
			}
		}

		return 0;
	}

	@Override
	public long writeBackupTimeBucketEvents(OutputStream outputStream, String customer, String bucket, DateTime dateTime)
	{
		ChannelMetaData channelMetaData = bucketMetaDataCache.getBucketMetaData(customer, bucket, false);

		if (channelMetaData != null)
		{
			Channel channel = ropes.get(channelMetaData);

			if (channel != null)
			{
				Batch batch = channel.getBackupTimeBucket(timePeriodUtils.nearestPeriodCeiling(dateTime));

				if (batch != null)
				{
					return timeBucketEventsToStream(outputStream, batch);
				}
			}
		}

		return 0;
	}

	private long timeBucketEventsToStream(OutputStream outputStream, Batch batch)
	{
		batch.eventsToJson(outputStream);
		return batch.getEvents().size();
	}

	@Override
	public BatchMetaData getTimeBucketMetaData(String customer, String bucket, DateTime dateTime)
	{
		ChannelMetaData channelMetaData = bucketMetaDataCache.getBucketMetaData(customer, bucket, false);
		return ropes.get(channelMetaData).getTimeBucket(timePeriodUtils.nearestPeriodCeiling(dateTime)).getMetaData();
	}

	@Override
	public BatchMetaData getBackupTimeBucketMetaData(String customer, String bucket, DateTime dateTime)
	{
		ChannelMetaData channelMetaData = bucketMetaDataCache.getBucketMetaData(customer, bucket, false);
		return ropes.get(channelMetaData).getBackupTimeBucket(timePeriodUtils.nearestPeriodCeiling(dateTime)).getMetaData();
	}

	@Override
	public void shutdown()
	{
		clusterService.getRegistrationService().unRegisterAsRopeManager();
	}
}
