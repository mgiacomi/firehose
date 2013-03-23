package com.gltech.scale.core.writer;

import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.ganglia.*;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.gltech.scale.core.cluster.BatchPeriodMapper;
import com.gltech.scale.core.cluster.ClusterService;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.storage.ChannelCache;
import com.gltech.scale.util.Props;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class StorageWriteManagerImpl implements StorageWriteManager
{
	private static final Logger logger = LoggerFactory.getLogger(StorageWriteManagerImpl.class);
	private volatile boolean shutdown = false;
	private volatile boolean confirmShutdown = false;
	private Props props = Props.getProps();
	private Injector injector;
	private ClusterService clusterService;
	private ChannelCache channelCache;
	private Timer collectBatchTimer = new Timer();
	private TimerMap timerMap = new TimerMap();
	private int periodSeconds;

	@Inject
	public StorageWriteManagerImpl(ClusterService clusterService, ChannelCache channelCache)
	{
		this.clusterService = clusterService;
		this.channelCache = channelCache;
		this.periodSeconds = props.get("period_seconds", Defaults.PERIOD_SECONDS);

		String groupName = "Storage Writer";
		MonitoringPublisher.getInstance().register(new PublishMetric("CollectBatch.Count", groupName, "count", new TimerCountPublisher("", collectBatchTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric("CollectBatch.AvgTime", groupName, "avg time in millis", new TimerAveragePublisher("", collectBatchTimer)));
		MonitoringPublisher.getInstance().register(new TimerMapPublishMetricGroup(groupName, timerMap));

		// Register the event service with the coordination service
		clusterService.getRegistrationService().registerAsStorageWriter();
	}

	@Override
	public void run()
	{
		int checkForWorkInterval = props.get("storage_writer.check_for_work_interval_secs", Defaults.STORAGE_WRITER_CHECK_FOR_WORK_INTERVAL_SECS);
		int activeStorageWriters = props.get("storage_writer.active_collectors", Defaults.STORAGE_WRITER_ACTIVE_WRITERS);

		TransferQueue<Runnable> queue = new LinkedTransferQueue<>();
		ThreadPoolExecutor threadPoolExecutor = new TimerThreadPoolExecutor(activeStorageWriters, activeStorageWriters, 1, TimeUnit.MINUTES, queue, new StorageWriterThreadFactory(), collectBatchTimer);
		logger.info("ThreadPoolExecutor started with " + activeStorageWriters + " active collectors.");

		try
		{
			while (!shutdown)
			{
				if (threadPoolExecutor.getActiveCount() < activeStorageWriters)
				{
					BatchPeriodMapper batchPeriodMapper = clusterService.getOldestCollectibleBatch();

					if (batchPeriodMapper != null)
					{
						try
						{
							ChannelMetaData channelMetaData = channelCache.getChannelMetaData(batchPeriodMapper.getChannelName(), true);

							BatchWriter batchWriter = injector.getInstance(BatchWriter.class);
							batchWriter.assign(channelMetaData, batchPeriodMapper.getNearestPeriodCeiling());

							// Get a Timer from the timermap based on the bucket being collected.
							String timerName = batchPeriodMapper.getChannelName() + "/" + Integer.toString(periodSeconds);
							batchWriter.setTimer(timerMap.get(timerName));

							threadPoolExecutor.submit(batchWriter);
						}
						catch (Exception e)
						{
							logger.error("StorageWriter error: {}|{}", batchPeriodMapper.getChannelName(), batchPeriodMapper.getNearestPeriodCeiling().toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")), e);
							continue;
						}

						logger.debug("BatchMataData submitted for collection: {}|{}", batchPeriodMapper.getChannelName(), batchPeriodMapper.getNearestPeriodCeiling().toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")));

						continue;
					}
				}
				else
				{
					logger.debug("Could not take on new work.  All threads busy. ActiveWriters={}", threadPoolExecutor.getActiveCount());
				}

				TimeUnit.SECONDS.sleep(checkForWorkInterval);
			}

			int waitForShutdown = props.get("storage_writer.wait_for_shutdown_mins", Defaults.STORAGE_WRITER_WAIT_FOR_SHUTDOWN_MINS);

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
			logger.error("StorageWriter was inturrupted.", e);
		}

		confirmShutdown = true;
	}

	@Override
	public void shutdown()
	{
		shutdown = true;

		clusterService.getRegistrationService().unRegisterAsStorageWriter();

		if (props.get("storage_writer.clean_shutdown", Defaults.STORAGE_WRITER_CLEAN_SHUTDOWN))
		{
			while (!confirmShutdown)
			{
				try
				{
					logger.info("Waiting to shutdown...");
					TimeUnit.MINUTES.sleep(1);
				}
				catch (InterruptedException e)
				{
					// Don't worry about it, just die.
				}
			}
		}

		logger.warn("StorageWriter has been shutdown.");
	}

	@Override
	public void setInjector(Injector injector)
	{
		this.injector = injector;
	}

	private class StorageWriterThreadFactory implements ThreadFactory
	{
		public Thread newThread(Runnable r)
		{
			return new Thread(r, "StorageWriter");
		}
	}
}
