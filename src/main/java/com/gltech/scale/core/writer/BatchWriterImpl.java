package com.gltech.scale.core.writer;

import com.gc.iotools.stream.is.InputStreamFromOutputStream;
import com.gltech.scale.core.aggregator.AggregatorsByPeriod;
import com.gltech.scale.core.cluster.ChannelCoordinator;
import com.gltech.scale.core.model.BatchMetaData;
import com.gltech.scale.core.stats.AvgStatOverTime;
import com.gltech.scale.core.stats.CountStatOverTime;
import com.gltech.scale.core.storage.StorageClient;
import com.google.inject.Inject;
import com.gltech.scale.core.cluster.ClusterService;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.aggregator.PrimaryBackupSet;
import com.gltech.scale.core.aggregator.AggregatorRestClient;
import com.gltech.scale.core.model.ChannelMetaData;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class BatchWriterImpl implements BatchWriter
{
	private static final Logger logger = LoggerFactory.getLogger(BatchWriterImpl.class);
	private DateTime nearestPeriodCeiling;
	private ChannelMetaData channelMetaData;
	private AggregatorRestClient aggregatorRestClient;
	private StorageClient storageClient;
	private ClusterService clusterService;
	private ChannelCoordinator channelCoordinator;
	private String customerBatchPeriod;
	private AvgStatOverTime channelStat;
	private CountStatOverTime messagesWrittenStat;
	private CountStatOverTime bytesWrittenStat;

	@Inject
	public BatchWriterImpl(AggregatorRestClient aggregatorRestClient, StorageClient storageClient, ClusterService clusterService, ChannelCoordinator channelCoordinator)
	{
		this.aggregatorRestClient = aggregatorRestClient;
		this.storageClient = storageClient;
		this.clusterService = clusterService;
		this.channelCoordinator = channelCoordinator;
	}

	@Override
	public void assign(ChannelMetaData channelMetaData, DateTime nearestPeriodCeiling)
	{
		this.channelMetaData = channelMetaData;
		this.nearestPeriodCeiling = nearestPeriodCeiling;
		this.customerBatchPeriod = channelMetaData.getName() + "|" + nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")) + "|" + channelMetaData.isRedundant();
	}

	@Override
	public Object call() throws Exception
	{
		if (channelMetaData == null || nearestPeriodCeiling == null)
		{
			String error = "BatchWriter can not be started without configuring a ChannelMetaData and nearestPeriodCeiling first.";
			logger.error(error);
			throw new RuntimeException(error);
		}

		try
		{
			long start = System.nanoTime();
			channelStat.startTimer();

			String channelName = channelMetaData.getName();

			List<ServiceMetaData> aggregators = new ArrayList<>();

			AggregatorsByPeriod aggregatorsByPeriod = channelCoordinator.getAggregatorPeriodMatrix(nearestPeriodCeiling);

			for (PrimaryBackupSet primaryBackupSet : aggregatorsByPeriod.getPrimaryBackupSets())
			{
				if (channelMetaData.isRedundant())
				{
					BatchMetaData primaryMetaData = aggregatorRestClient.getBatchMetaData(primaryBackupSet.getPrimary(), channelName, nearestPeriodCeiling);

					aggregators.add(primaryBackupSet.getPrimary());

					if (primaryBackupSet.getBackup() != null)
					{
						BatchMetaData backupMetaData = aggregatorRestClient.getBackupBatchMetaData(primaryBackupSet.getBackup(), channelName, nearestPeriodCeiling);

						// If there is any discrepancy between the primary and backup rope for a double write bucket,
						// then pull both and let BatchStreamsManager figure it out. Otherwise only primary is used.
						if (primaryMetaData.getMessagesAdded() != backupMetaData.getMessagesAdded() || primaryMetaData.getBytes() != backupMetaData.getBytes())
						{
							aggregators.add(primaryBackupSet.getBackup());
							logger.warn("A discrepancy between primary and backup aggregators has been detected for {}", customerBatchPeriod);
						}
					}
				}
				else
				{
					aggregators.add(primaryBackupSet.getPrimary());

					if (primaryBackupSet.getBackup() != null)
					{
						aggregators.add(primaryBackupSet.getBackup());
					}
				}
			}

			final BatchStreamsManager batchStreamsManager = new BatchStreamsManager(channelMetaData, nearestPeriodCeiling);
			batchStreamsManager.setMessagesWrittenStat(messagesWrittenStat);
			batchStreamsManager.setBytesWrittenStat(bytesWrittenStat);

			// Register aggregator streams with the stream manager.
			for (ServiceMetaData aggregator : aggregators)
			{
				InputStream aggregatorStream = aggregatorRestClient.getBatchMessagesStream(aggregator, channelName, nearestPeriodCeiling);
				batchStreamsManager.registerInputStream(aggregatorStream);
			}

			final InputStreamFromOutputStream<Long> inputStream = new InputStreamFromOutputStream<Long>()
			{
				@Override
				public Long produce(final OutputStream outputStream) throws Exception
				{
					return batchStreamsManager.writeMessages(outputStream);
				}
			};

			// Write the stream to the storage service.
			storageClient.putMessages(channelMetaData, nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")), inputStream);
			inputStream.close();

			// Remove batch references from the coordination service.
			clusterService.clearBatchMetaData(channelMetaData, nearestPeriodCeiling);

			// Data has been written so remove it from all active aggregator.
			for (PrimaryBackupSet primaryBackupSet : aggregatorsByPeriod.getPrimaryBackupSets())
			{
				// Aggregator can now remove the batch
				try
				{
					aggregatorRestClient.clearBatch(primaryBackupSet.getPrimary(), channelName, nearestPeriodCeiling);
				}
				catch (Exception e)
				{
					logger.error("Failed to clear the batch from aggregator host={} port={}", primaryBackupSet.getPrimary().getListenAddress(), primaryBackupSet.getPrimary().getListenPort(), e);
				}

				if (primaryBackupSet.getBackup() != null)
				{
					try
					{
						aggregatorRestClient.clearBatch(primaryBackupSet.getBackup(), channelName, nearestPeriodCeiling);
					}
					catch (Exception e)
					{
						logger.error("Failed to clear the backup batch from aggregator host={} port={}", primaryBackupSet.getPrimary().getListenAddress(), primaryBackupSet.getPrimary().getListenPort(), e);
					}
				}
			}

			long completedIn = System.nanoTime() - start;
			channelStat.stopTimer();

			logger.info("Completed writing Batch for " + customerBatchPeriod + " in " + completedIn / 1000000 + "ms");
		}
		catch (Exception e)
		{
			logger.error("Failed to write Batch. " + customerBatchPeriod, e);
			clusterService.clearStorageWriterLock(channelMetaData, nearestPeriodCeiling);
		}

		return null;
	}

	@Override
	public void setChannelStat(AvgStatOverTime channelStat)
	{
		this.channelStat = channelStat;
	}

	@Override
	public void setMessagesWrittenStat(CountStatOverTime messagesWrittenStat)
	{
		this.messagesWrittenStat = messagesWrittenStat;
	}

	@Override
	public void setBytesWrittenStat(CountStatOverTime bytesWrittenStat)
	{
		this.bytesWrittenStat = bytesWrittenStat;
	}
}
