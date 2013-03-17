package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.model.BatchMetaData;
import com.gltech.scale.core.model.Message;
import com.gltech.scale.core.model.Batch;
import com.gltech.scale.util.StreamDelimiter;
import com.google.inject.Inject;
import com.gltech.scale.core.cluster.ClusterService;
import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.storage.ChannelCache;
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
	private ConcurrentMap<ChannelMetaData, Channel> channels = new ConcurrentHashMap<>();
	private ChannelCache channelCache;
	private ClusterService clusterService;
	private TimePeriodUtils timePeriodUtils;
	private StreamDelimiter streamDelimiter;

	@Inject
	public AggregatorImpl(ChannelCache channelCache, ClusterService clusterService, TimePeriodUtils timePeriodUtils, StreamDelimiter streamDelimiter)
	{
		this.channelCache = channelCache;
		this.clusterService = clusterService;
		this.timePeriodUtils = timePeriodUtils;
		this.streamDelimiter = streamDelimiter;

		// Register the aggregator with the coordination service, so that collectors can find us
		clusterService.getRegistrationService().registerAsAggregator();
	}

	@Override
	public void addMessage(String channelName, byte[] bytes)
	{
		ChannelMetaData channelMetaData = channelCache.getChannelMetaData(channelName, true);
		Channel channel = channels.get(channelMetaData);

		if (channel == null)
		{
			Channel newChannel = new ChannelStats(new ChannelImpl(channelMetaData, clusterService, timePeriodUtils));
			channel = channels.putIfAbsent(channelMetaData, newChannel);
			if (channel == null)
			{
				channel = newChannel;
				logger.info("Creating Channel: " + channelName);
			}
		}

		channel.addMessage(bytes);
	}

	@Override
	public void addBackupMessage(String channelName, byte[] bytes)
	{
		ChannelMetaData channelMetaData = channelCache.getChannelMetaData(channelName, true);

		Channel channel = channels.get(channelMetaData);

		if (channel == null)
		{
			logger.info("Creating Channel: "+ channelName);
			Channel newChannel = new ChannelStats(new ChannelImpl(channelMetaData, clusterService, timePeriodUtils));
			channel = channels.putIfAbsent(channelMetaData, newChannel);
			if (channel == null)
			{
				channel = newChannel;
			}
		}

		channel.addBackupMessage(bytes);
	}

	@Override
	public void clear(String channelName, DateTime dateTime)
	{
		ChannelMetaData channelMetaData = channelCache.getChannelMetaData(channelName, false);
		Channel channel = channels.get(channelMetaData);

		if (channel != null)
		{
			channel.clear(timePeriodUtils.nearestPeriodCeiling(dateTime));
			if (channelMetaData.isRedundant())
			{
				channel.clearBackup(timePeriodUtils.nearestPeriodCeiling(dateTime));
			}
		}
	}

	@Override
	public List<Batch> getActiveBatches()
	{
		List<Batch> activeBatches = new ArrayList<>();

		for (Channel channel : channels.values())
		{
			for (Batch batch : channel.getBatches())
			{
				activeBatches.add(batch);
			}
		}

		return Collections.unmodifiableList(activeBatches);
	}

	@Override
	public List<Batch> getActiveBackupBatches()
	{
		List<Batch> activeBackupBatches = new ArrayList<>();

		for (Channel channel : channels.values())
		{
			for (Batch batch : channel.getBackupBatches())
			{
				activeBackupBatches.add(batch);
			}
		}

		return Collections.unmodifiableList(activeBackupBatches);
	}

	@Override
	public long writeBatchMessages(OutputStream outputStream, String ChannelName, DateTime dateTime)
	{
		ChannelMetaData channelMetaData = channelCache.getChannelMetaData(ChannelName, false);

		if (channelMetaData != null)
		{
			Channel channel = channels.get(channelMetaData);

			if (channel != null)
			{
				Batch batch = channels.get(channelMetaData).getBatch(timePeriodUtils.nearestPeriodCeiling(dateTime));

				if (batch != null)
				{
					return timeBucketEventsToStream(outputStream, batch);
				}
			}
		}

		return 0;
	}

	@Override
	public long writeBackupBatchMessages(OutputStream outputStream, String ChannelName, DateTime dateTime)
	{
		ChannelMetaData channelMetaData = channelCache.getChannelMetaData(ChannelName, false);

		if (channelMetaData != null)
		{
			Channel channel = channels.get(channelMetaData);

			if (channel != null)
			{
				Batch batch = channel.getBackupBatch(timePeriodUtils.nearestPeriodCeiling(dateTime));

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
		for(byte[] bytes : batch.getMessages())
		{
			streamDelimiter.write(outputStream, bytes);
		}
		return batch.getMessages().size();
	}

	@Override
	public BatchMetaData getBatchMetaData(String channelName, DateTime dateTime)
	{
		ChannelMetaData channelMetaData = channelCache.getChannelMetaData(channelName, false);
		return channels.get(channelMetaData).getBatch(timePeriodUtils.nearestPeriodCeiling(dateTime)).getMetaData();
	}

	@Override
	public BatchMetaData getBatchBucketMetaData(String channelName, DateTime dateTime)
	{
		ChannelMetaData channelMetaData = channelCache.getChannelMetaData(channelName, false);
		return channels.get(channelMetaData).getBackupBatch(timePeriodUtils.nearestPeriodCeiling(dateTime)).getMetaData();
	}

	@Override
	public void shutdown()
	{
		clusterService.getRegistrationService().unRegisterAsAggregator();
	}
}
