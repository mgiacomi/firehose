package com.gltech.scale.core.collector;

import com.gc.iotools.stream.is.InputStreamFromOutputStream;
import com.google.inject.Inject;
import com.gltech.scale.core.coordination.CoordinationService;
import com.gltech.scale.core.coordination.RopeCoordinator;
import com.gltech.scale.core.coordination.registration.ServiceMetaData;
import com.gltech.scale.core.monitor.Timer;
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

public class TimeBucketCollectorImpl implements TimeBucketCollector
{
	private static final Logger logger = LoggerFactory.getLogger("com.lokiscale.collector.TimeBucketCollectorSingleAndDouble");
	private DateTime nearestPeriodCeiling;
	private BucketMetaData bucketMetaData;
	private RopeManagerRestClient ropeManagerRestClient;
	private StorageServiceClient storageServiceClient;
	private CoordinationService coordinationService;
	private RopeCoordinator ropeCoordinator;
	private Timer timer;

	@Inject
	public TimeBucketCollectorImpl(RopeManagerRestClient ropeManagerRestClient, StorageServiceClient storageServiceClient, CoordinationService coordinationService, RopeCoordinator ropeCoordinator)
	{
		this.ropeManagerRestClient = ropeManagerRestClient;
		this.storageServiceClient = storageServiceClient;
		this.coordinationService = coordinationService;
		this.ropeCoordinator = ropeCoordinator;
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

			RopeManagersByPeriod ropeManagersByPeriod = ropeCoordinator.getRopeManagerPeriodMatrix(nearestPeriodCeiling);

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

			final TimeBucketStreamsManager timeBucketStreamsManager = new TimeBucketStreamsManager(bucketMetaData, nearestPeriodCeiling);

			// Register rope manager streams with the stream manager.
			for (ServiceMetaData ropeManager : ropeManagers)
			{
				InputStream ropeStream = ropeManagerRestClient.getTimeBucketEventsStream(ropeManager, customer, bucket, nearestPeriodCeiling);
				timeBucketStreamsManager.registerInputStream(ropeStream);
			}

			final InputStreamFromOutputStream<Long> inputStream = new InputStreamFromOutputStream<Long>()
			{
				@Override
				public Long produce(final OutputStream outputStream) throws Exception
				{
					return timeBucketStreamsManager.writeEvents(outputStream);
				}
			};

			// Write the stream to the storage service.
			ServiceMetaData storageService = coordinationService.getRegistrationService().getStorageServiceRoundRobin();
			storageServiceClient.put(storageService, customer, bucket, nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")), inputStream);
			inputStream.close();

			// Remove time bucket references from the coordination service.
			coordinationService.clearTimeBucketMetaData(bucketMetaData, nearestPeriodCeiling);

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
			coordinationService.clearCollectorLock(bucketMetaData, nearestPeriodCeiling);
		}

		return null;
	}

	public void setTimer(Timer timer)
	{
		this.timer = timer;
	}
}