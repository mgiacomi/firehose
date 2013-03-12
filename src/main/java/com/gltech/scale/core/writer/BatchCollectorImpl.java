package com.gltech.scale.core.writer;

import com.gc.iotools.stream.is.InputStreamFromOutputStream;
import com.gltech.scale.core.aggregator.AggregatorsByPeriod;
import com.gltech.scale.core.cluster.ChannelCoordinator;
import com.gltech.scale.core.model.BatchMetaData;
import com.google.inject.Inject;
import com.gltech.scale.core.cluster.ClusterService;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.ganglia.Timer;
import com.gltech.scale.core.aggregator.PrimaryBackupSet;
import com.gltech.scale.core.aggregator.AggregatorRestClient;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.storage.StorageServiceClient;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class BatchCollectorImpl implements BatchCollector
{
	private static final Logger logger = LoggerFactory.getLogger(BatchCollectorImpl.class);
	private DateTime nearestPeriodCeiling;
	private ChannelMetaData channelMetaData;
	private AggregatorRestClient aggregatorRestClient;
	private StorageServiceClient storageServiceClient;
	private ClusterService clusterService;
	private ChannelCoordinator channelCoordinator;
	private Timer timer;

	@Inject
	public BatchCollectorImpl(AggregatorRestClient aggregatorRestClient, StorageServiceClient storageServiceClient, ClusterService clusterService, ChannelCoordinator channelCoordinator)
	{
		this.aggregatorRestClient = aggregatorRestClient;
		this.storageServiceClient = storageServiceClient;
		this.clusterService = clusterService;
		this.channelCoordinator = channelCoordinator;
	}

	@Override
	public void assign(ChannelMetaData channelMetaData, DateTime nearestPeriodCeiling)
	{
		this.channelMetaData = channelMetaData;
		this.nearestPeriodCeiling = nearestPeriodCeiling;
	}

	@Override
	public Object call() throws Exception
	{
		if (channelMetaData == null || nearestPeriodCeiling == null)
		{
			String error = "TimeBucketCollector can not be started without configuring a BucketMetaData and nearestPeriodCeiling first.";
			logger.error(error);
			throw new RuntimeException(error);
		}

		try
		{
			long start = System.nanoTime();

			String customer = channelMetaData.getCustomer();
			String bucket = channelMetaData.getBucket();

			List<ServiceMetaData> ropeManagers = new ArrayList<>();

			AggregatorsByPeriod aggregatorsByPeriod = channelCoordinator.getRopeManagerPeriodMatrix(nearestPeriodCeiling);

			for (PrimaryBackupSet primaryBackupSet : aggregatorsByPeriod.getPrimaryBackupSets())
			{
				if (channelMetaData.isDoubleWrite())
				{
					BatchMetaData primaryMetaData = aggregatorRestClient.getTimeBucketMetaData(primaryBackupSet.getPrimary(), customer, bucket, nearestPeriodCeiling);
					BatchMetaData backupMetaData = aggregatorRestClient.getTimeBucketMetaData(primaryBackupSet.getPrimary(), customer, bucket, nearestPeriodCeiling);

					ropeManagers.add(primaryBackupSet.getPrimary());

					// If there is any discrepancy between the primary and backup rope for a double write bucket,
					// then pull both and let TimeBucketStreamsManager figure it out. Otherwise only primary is used.
					if (primaryMetaData.getEventsAdded() != backupMetaData.getEventsAdded() || primaryMetaData.getBytes() != backupMetaData.getBytes())
					{
						ropeManagers.add(primaryBackupSet.getBackup());
					}
				}
				else
				{
					ropeManagers.add(primaryBackupSet.getPrimary());

					if (primaryBackupSet.getBackup() != null)
					{
						ropeManagers.add(primaryBackupSet.getBackup());
					}
				}
			}

			final BatchStreamsManager batchStreamsManager = new BatchStreamsManager(channelMetaData, nearestPeriodCeiling);

			// Register rope manager streams with the stream manager.
			for (ServiceMetaData ropeManager : ropeManagers)
			{
				InputStream ropeStream = aggregatorRestClient.getTimeBucketEventsStream(ropeManager, customer, bucket, nearestPeriodCeiling);
				batchStreamsManager.registerInputStream(ropeStream);
			}

			final InputStreamFromOutputStream<Long> inputStream = new InputStreamFromOutputStream<Long>()
			{
				@Override
				public Long produce(final OutputStream outputStream) throws Exception
				{
					return batchStreamsManager.writeEvents(outputStream);
				}
			};

			// Write the stream to the storage service.
			ServiceMetaData storageService = clusterService.getRegistrationService().getStorageServiceRoundRobin();
			storageServiceClient.put(storageService, customer, bucket, nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")), inputStream);
			inputStream.close();

			// Remove time bucket references from the coordination service.
			clusterService.clearTimeBucketMetaData(channelMetaData, nearestPeriodCeiling);

			// Data has been written so remove it from all active rope managers.
			for (ServiceMetaData ropeManager : ropeManagers)
			{
				// RopeManager can now remove the time bucket
				aggregatorRestClient.clearTimeBucket(ropeManager, customer, bucket, nearestPeriodCeiling);
			}

			long completedIn = System.nanoTime() - start;

			logger.info("Completed collecting TimeBucket for " + channelMetaData.getCustomer() + "|" + channelMetaData.getBucket() + "|" + nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")) + " in " + completedIn / 1000000 + "ms");

			if (timer != null)
			{
				timer.add(completedIn, inputStream.getResult());
			}
		}
		catch (Exception e)
		{
			logger.error("Failed to collect TimeBucket. " + nearestPeriodCeiling + ", " + channelMetaData.toString(), e);
			clusterService.clearCollectorLock(channelMetaData, nearestPeriodCeiling);
		}

		return null;
	}

	public void setTimer(Timer timer)
	{
		this.timer = timer;
	}
}
