package com.gltech.scale.core.writer;

import com.gltech.scale.ganglia.*;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.gltech.scale.core.cluster.BatchPeriodMapper;
import com.gltech.scale.core.cluster.ClusterService;
import com.gltech.scale.core.monitor.*;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.storage.BucketMetaDataCache;
import com.gltech.scale.util.Props;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class StorageWriteManagerWithCoordination implements StorageWriteManager
{
	private static final Logger logger = LoggerFactory.getLogger(StorageWriteManagerWithCoordination.class);
	private volatile boolean shutdown = false;
	private volatile boolean confirmShutdown = false;
	private Props props = Props.getProps();
	private Injector injector;
	private ClusterService clusterService;
	private BucketMetaDataCache bucketMetaDataCache;
	private Timer collectTimeBucketTimer = new Timer();
	private TimerMap timerMap = new TimerMap();
	private int periodSeconds;

	@Inject
	public StorageWriteManagerWithCoordination(ClusterService clusterService, BucketMetaDataCache bucketMetaDataCache)
	{
		this.clusterService = clusterService;
		this.bucketMetaDataCache = bucketMetaDataCache;
		this.periodSeconds = props.get("coordination.period_seconds", 5);

		String groupName = "Loki Collector";
		MonitoringPublisher.getInstance().register(new PublishMetric("CollectTimeBucket.Count", groupName, "count", new TimerCountPublisher("", collectTimeBucketTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric("CollectTimeBucket.AvgTime", groupName, "avg time in millis", new TimerAveragePublisher("", collectTimeBucketTimer)));
		MonitoringPublisher.getInstance().register(new TimerMapPublishMetricGroup(groupName, timerMap));

		// Register the event service with the coordination service
		clusterService.getRegistrationService().registerAsCollectorManager();
	}

	@Override
	public void run()
	{
		int checkForWorkInterval = props.get("collector.manager.check_for_work_interval_secs", 5) * 1000;
		int activeCollectors = props.get("collector.manager.active_collectors", 100);

		TransferQueue<Runnable> queue = new LinkedTransferQueue<>();
		ThreadPoolExecutor threadPoolExecutor = new TimerThreadPoolExecutor(activeCollectors, activeCollectors, 1, TimeUnit.MINUTES, queue, new CollectorManagerThreadFactory(), collectTimeBucketTimer);
		logger.info("ThreadPoolExecutor started with " + activeCollectors + " active collectors.");

		try
		{
			while (!shutdown)
			{
				if (threadPoolExecutor.getActiveCount() < activeCollectors)
				{
					BatchPeriodMapper batchPeriodMapper = clusterService.getOldestCollectibleTimeBucket();

					if (batchPeriodMapper != null)
					{
						try
						{
							ChannelMetaData channelMetaData = bucketMetaDataCache.getBucketMetaData(batchPeriodMapper.getCustomer(), batchPeriodMapper.getBucket(), true);

							BatchCollector batchCollector = injector.getInstance(BatchCollector.class);
							batchCollector.assign(channelMetaData, batchPeriodMapper.getNearestPeriodCeiling());

							// Get a Timer from the timermap based on the bucket being collected.
							String timerName = batchPeriodMapper.getCustomer() + "/" + batchPeriodMapper.getBucket() + "/" + Integer.toString(periodSeconds);
							batchCollector.setTimer(timerMap.get(timerName));

							threadPoolExecutor.submit(batchCollector);
						}
						catch (Exception e)
						{
							logger.error("Collection error: {}|{}|{}", batchPeriodMapper.getCustomer(), batchPeriodMapper.getBucket(), batchPeriodMapper.getNearestPeriodCeiling().toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")), e);
							continue;
						}

						logger.debug("TimeBucketMataData submitted for collection: {}|{}|{}", batchPeriodMapper.getCustomer(), batchPeriodMapper.getBucket(), batchPeriodMapper.getNearestPeriodCeiling().toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")));

						continue;
					}
				}
				else
				{
					logger.debug("Could not take on new work.  All threads busy. ActiveCollectors={}", threadPoolExecutor.getActiveCount());
				}

				Thread.sleep(checkForWorkInterval);
			}

			int waitForShutdown = props.get("collector.manager.wait_for_shutdown_mins", 15);

			logger.info("ThreadPoolExecutor shutdown requested. " + threadPoolExecutor.getActiveCount() + " threads still active.");
			threadPoolExecutor.shutdown();

			if (threadPoolExecutor.awaitTermination(waitForShutdown, TimeUnit.MINUTES))
			{
				logger.info("ThreadPoolExecutor has been cleanly shutdown.");
			}
			else
			{
				logger.info("ThreadPoolExecutor has not been shutdown " + threadPoolExecutor.getActiveCount() + " threads still active.");
			}
		}
		catch (InterruptedException e)
		{
			logger.error("CollectorManager was inturrupted.", e);
		}

		confirmShutdown = true;
	}

	@Override
	public void shutdown()
	{
		shutdown = true;

		clusterService.getRegistrationService().unRegisterAsCollectorManager();

		if (props.get("collector.manager.clean_shutdown", false))
		{
			while (!confirmShutdown)
			{
				try
				{
					logger.info("Waiting to shutdown...");
					Thread.sleep(1000);
				}
				catch (InterruptedException e)
				{
					// Don't worry about it, just die.
				}
			}
		}

		logger.warn("CollectorManager has been shutdown.");
	}

	@Override
	public void setInjector(Injector injector)
	{
		this.injector = injector;
	}

	private class CollectorManagerThreadFactory implements ThreadFactory
	{
		public Thread newThread(Runnable r)
		{
			return new Thread(r, "TimeBucketCollector");
		}
	}
}
