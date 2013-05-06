package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.model.BatchMetaData;
import com.gltech.scale.core.model.Batch;
import com.gltech.scale.core.stats.StatsManager;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.gltech.scale.core.cluster.ClusterService;
import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.storage.ChannelCache;
import com.google.protobuf.CodedOutputStream;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
	private StatsManager statsManager;

	@Inject
	public AggregatorImpl(ChannelCache channelCache, ClusterService clusterService, TimePeriodUtils timePeriodUtils, StatsManager statsManager)
	{
		this.channelCache = channelCache;
		this.clusterService = clusterService;
		this.timePeriodUtils = timePeriodUtils;
		this.statsManager = statsManager;

		// Register the aggregator with the coordination service, so that the storage writer can find us
		clusterService.getRegistrationService().registerAsAggregator();
	}

	@Override
	public void addMessage(String channelName, byte[] bytes)
	{
		ChannelMetaData channelMetaData = channelCache.getChannelMetaData(channelName, true);
		Channel channel = channels.get(channelMetaData);

		if (channel == null)
		{
			Channel newChannel = new ChannelStats(new ChannelImpl(channelMetaData, clusterService, timePeriodUtils), statsManager);
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
			logger.info("Creating Channel: " + channelName);
			Channel newChannel = new ChannelStats(new ChannelImpl(channelMetaData, clusterService, timePeriodUtils), statsManager);
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
			DateTime nearestPeriodCeiling = timePeriodUtils.nearestPeriodCeiling(dateTime);

			if (channel != null)
			{
				Batch batch = channels.get(channelMetaData).getBatch(nearestPeriodCeiling);

				if (batch != null)
				{
					long messagesWritten = batchMessagesToStream(outputStream, batch);
					logger.warn("Collected {} primary messages from batch: {}", messagesWritten, channelMetaData.getName() + "|" + nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")));
					return messagesWritten;
				}
			}
			else
			{
				logger.warn("Trying to collect primary messages from a null batch: {}", channelMetaData.getName() + "|" + nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")));
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
			DateTime nearestPeriodCeiling = timePeriodUtils.nearestPeriodCeiling(dateTime);

			if (channel != null)
			{
				Batch batch = channel.getBackupBatch(timePeriodUtils.nearestPeriodCeiling(dateTime));

				if (batch != null)
				{
					long messagesWritten = batchMessagesToStream(outputStream, batch);
					logger.warn("Collected {} backup messages from batch: {}", messagesWritten, channelMetaData.getName() + "|" + nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")));
					return messagesWritten;
				}
			}
			else
			{
				logger.warn("Trying to collect backup messages from a null batch: {}", channelMetaData.getName() + "|" + nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")));
			}
		}

		return 0;
	}

	private long batchMessagesToStream(OutputStream outputStream, Batch batch)
	{
		CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(outputStream);

		try
		{
			for (byte[] bytes : batch.getMessages())
			{
				codedOutputStream.writeRawVarint32(bytes.length);
				codedOutputStream.writeRawBytes(bytes);

			}
			codedOutputStream.flush();
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}

		return batch.getMessages().size();
	}

	@Override
	public BatchMetaData getBatchMetaData(String channelName, DateTime dateTime)
	{
		ChannelMetaData channelMetaData = channelCache.getChannelMetaData(channelName, false);

		try
		{
			return channels.get(channelMetaData).getBatch(timePeriodUtils.nearestPeriodCeiling(dateTime)).getMetaData();
		}
		catch (NullPointerException e)
		{
			return null;
		}
	}

	@Override
	public BatchMetaData getBatchBucketMetaData(String channelName, DateTime dateTime)
	{
		ChannelMetaData channelMetaData = channelCache.getChannelMetaData(channelName, false);

		try
		{
			return channels.get(channelMetaData).getBackupBatch(timePeriodUtils.nearestPeriodCeiling(dateTime)).getMetaData();
		}
		catch (NullPointerException e)
		{
			return null;
		}
	}

	@Override
	public void shutdown()
	{
		clusterService.getRegistrationService().unRegisterAsAggregator();
	}
}
