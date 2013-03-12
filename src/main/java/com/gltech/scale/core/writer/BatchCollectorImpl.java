package com.gltech.scale.core.writer;

import com.gc.iotools.stream.is.InputStreamFromOutputStream;
import com.gltech.scale.core.cluster.ChannelCoordinator;
import com.google.inject.Inject;
import com.gltech.scale.core.cluster.ClusterService;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.ganglia.Timer;
import com.gltech.scale.core.rope.PrimaryBackupSet;
import com.gltech.scale.core.rope.RopeManagerRestClient;
import com.gltech.scale.core.rope.RopeManagersByPeriod;
import com.gltech.scale.core.rope.TimeBucketMetaData;
import com.gltech.scale.core.storage.BucketMetaData;
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
	private static final Logger logger = LoggerFactory.getLogger("com.lokiscale.collector.TimeBucketCollectorSingleAndDouble");
	private DateTime nearestPeriodCeiling;
	private BucketMetaData bucketMetaData;
	private RopeManagerRestClient ropeManagerRestClient;
	private StorageServiceClient storageServiceClient;
	private ClusterService clusterService;
	private ChannelCoordinator channelCoordinator;
	private Timer timer;

	@Inject
	public BatchCollectorImpl(RopeManagerRestClient ropeManagerRestClient, StorageServiceClient storageServiceClient, ClusterService clusterService, ChannelCoordinator channelCoordinator)
	{
		this.ropeManagerRestClient = ropeManagerRestClient;
		this.storageServiceClient = storageServiceClient;
		this.clusterService = clusterService;
		this.channelCoordinator = channelCoordinator;
	}

	@Override
	public void assign(BucketMetaData bucketMetaData, DateTime nearestPeriodCeiling)
	{
		this.bucketMetaData = bucketMetaData;
		this.nearestPeriodCeiling = nearestPeriodCeiling;
	}

	@Override
	public Object call() throws Exception
	{
		if (bucketMetaData == null || nearestPeriodCeiling == null)
		{
			String error = "TimeBucketCollector can not be started without configuring a BucketMetaData and nearestPeriodCeiling first.";
			logger.error(error);
			throw new RuntimeException(error);
		}

		try
		{
			long start = System.nanoTime();

			String customer = bucketMetaData.getCustomer();
			String bucket = bucketMetaData.getBucket();

			List<ServiceMetaData> ropeManagers = new ArrayList<>();

			RopeManagersByPeriod ropeManagersByPeriod = channelCoordinator.getRopeManagerPeriodMatrix(nearestPeriodCeiling);

			for (PrimaryBackupSet primaryBackupSet : ropeManagersByPeriod.getPrimaryBackupSets())
			{
				if (bucketMetaData.isDoubleWrite())
				{
					TimeBucketMetaData primaryMetaData = ropeManagerRestClient.getTimeBucketMetaData(primaryBackupSet.getPrimary(), customer, bucket, nearestPeriodCeiling);
					TimeBucketMetaData backupMetaData = ropeManagerRestClient.getTimeBucketMetaData(primaryBackupSet.getPrimary(), customer, bucket, nearestPeriodCeiling);

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

			final BatchStreamsManager batchStreamsManager = new BatchStreamsManager(bucketMetaData, nearestPeriodCeiling);

			// Register rope manager streams with the stream manager.
			for (ServiceMetaData ropeManager : ropeManagers)
			{
				InputStream ropeStream = ropeManagerRestClient.getTimeBucketEventsStream(ropeManager, customer, bucket, nearestPeriodCeiling);
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
			clusterService.clearTimeBucketMetaData(bucketMetaData, nearestPeriodCeiling);

			// Data has been written so remove it from all active rope managers.
			for (ServiceMetaData ropeManager : ropeManagers)
			{
				// RopeManager can now remove the time bucket
				ropeManagerRestClient.clearTimeBucket(ropeManager, customer, bucket, nearestPeriodCeiling);
			}

			long completedIn = System.nanoTime() - start;

			logger.info("Completed collecting TimeBucket for " + bucketMetaData.getCustomer() + "|" + bucketMetaData.getBucket() + "|" + nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")) + " in " + completedIn / 1000000 + "ms");

			if (timer != null)
			{
				timer.add(completedIn, inputStream.getResult());
			}
		}
		catch (Exception e)
		{
			logger.error("Failed to collect TimeBucket. " + nearestPeriodCeiling + ", " + bucketMetaData.toString(), e);
			clusterService.clearCollectorLock(bucketMetaData, nearestPeriodCeiling);
		}

		return null;
	}

	public void setTimer(Timer timer)
	{
		this.timer = timer;
	}
}
