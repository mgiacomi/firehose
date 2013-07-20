package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.cluster.ClusterService;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.util.Props;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ChannelImpl implements Channel
{
	private static final Logger logger = LoggerFactory.getLogger(ChannelImpl.class);
	private final ConcurrentMap<DateTime, Batch> batches = new ConcurrentHashMap<>();
	private final ConcurrentMap<DateTime, Batch> backupBatches = new ConcurrentHashMap<>();
	private final ChannelMetaData channelMetaData;
	private final ClusterService clusterService;
	private Props props = Props.getProps();

	public ChannelImpl(ChannelMetaData channelMetaData, ClusterService clusterService)
	{
		this.channelMetaData = channelMetaData;
		this.clusterService = clusterService;
	}

	@Override
	public void addMessage(byte[] bytes, DateTime nearestPeriodCeiling)
	{
		Batch batch = batches.get(nearestPeriodCeiling);

		if (batch == null)
		{
			// Register batch for collection
			clusterService.registerBatch(channelMetaData, nearestPeriodCeiling);

			Batch newBatch;
			if ("memory".equals(props.get("channel_batch_type", Defaults.CHANNEL_BATCH_TYPE)))
			{
				newBatch = new BatchMemory(channelMetaData, nearestPeriodCeiling);
			}
			else
			{
				newBatch = new BatchNIOFile(channelMetaData, nearestPeriodCeiling);
			}

			batch = batches.putIfAbsent(nearestPeriodCeiling, newBatch);
			if (batch == null)
			{
				batch = newBatch;
				logger.info("Creating Batch {channelName=" + channelMetaData.getName() + "|" + nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")));
			}
		}

		batch.addMessage(bytes);
	}

	@Override
	public void addBackupMessage(byte[] bytes, DateTime nearestPeriodCeiling)
	{
		Batch batch = backupBatches.get(nearestPeriodCeiling);

		if (batch == null)
		{
			logger.info("Creating Backup Batch " + channelMetaData.getName() + "|" + nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")));

			// Register batch for collection
			clusterService.registerBatch(channelMetaData, nearestPeriodCeiling);

			Batch newBatch;
			if ("memory".equals(props.get("channel_batch_type", Defaults.CHANNEL_BATCH_TYPE)))
			{
				newBatch = new BatchMemory(channelMetaData, nearestPeriodCeiling);
			}
			else
			{
				newBatch = new BatchNIOFile(channelMetaData, nearestPeriodCeiling);
			}

			batch = backupBatches.putIfAbsent(nearestPeriodCeiling, newBatch);
			if (batch == null)
			{
				batch = newBatch;
			}
		}

		batch.addMessage(bytes);
	}

	@Override
	public ChannelMetaData getChannelMetaData()
	{
		return channelMetaData;
	}

	@Override
	public Collection<Batch> getBatches()
	{
		return Collections.unmodifiableCollection(batches.values());
	}

	@Override
	public Collection<Batch> getBackupBatches()
	{
		return Collections.unmodifiableCollection(backupBatches.values());
	}

	@Override
	public Batch getBatch(DateTime nearestPeriodCeiling)
	{
		return batches.get(nearestPeriodCeiling);
	}

	@Override
	public Batch getBackupBatch(DateTime nearestPeriodCeiling)
	{
		return backupBatches.get(nearestPeriodCeiling);
	}

	@Override
	public void clear(DateTime nearestPeriodCeiling)
	{
		if (batches.containsKey(nearestPeriodCeiling))
		{
			batches.get(nearestPeriodCeiling).clear();
			batches.remove(nearestPeriodCeiling);
			logger.info("Cleared Batch " + channelMetaData.getName() + "|" + nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")));
		}
	}

	@Override
	public void clearBackup(DateTime nearestPeriodCeiling)
	{
		if (backupBatches.containsKey(nearestPeriodCeiling))
		{
			backupBatches.get(nearestPeriodCeiling).clear();
			backupBatches.remove(nearestPeriodCeiling);
			logger.info("Cleared backup Batch " + channelMetaData.getName() + "|" + nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")));
		}
	}
}
