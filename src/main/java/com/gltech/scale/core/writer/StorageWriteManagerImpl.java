package com.gltech.scale.core.writer;

import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.core.stats.AvgStatOverTime;
import com.gltech.scale.core.stats.StatsManager;
import com.gltech.scale.core.stats.StatsThreadPoolExecutor;
import com.google.common.base.Throwables;
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
	private static ScheduledExecutorService scheduledWriterService;
	private final ThreadPoolExecutor threadPoolExecutor;
	private Props props = Props.getProps();
	private Injector injector;
	private ClusterService clusterService;
	private ChannelCache channelCache;
	private StatsManager statsManager;
	private int activeStorageWriters;
	private String groupName = "Storage Writer";

	@Inject
	public StorageWriteManagerImpl(ClusterService clusterService, ChannelCache channelCache, StatsManager statsManager)
	{
		this.clusterService = clusterService;
		this.channelCache = channelCache;
		this.statsManager = statsManager;

		AvgStatOverTime collectBatchAvgTime = statsManager.createAvgAndCountStat(groupName, "CollectBatch.AvgTime", "CollectBatch.Count");

		activeStorageWriters = props.get("storage_writer.active_collectors", Defaults.STORAGE_WRITER_ACTIVE_WRITERS);

		TransferQueue<Runnable> queue = new LinkedTransferQueue<>();
		threadPoolExecutor = new StatsThreadPoolExecutor(activeStorageWriters, activeStorageWriters, 1, TimeUnit.MINUTES, queue, new StorageWriterThreadFactory(), collectBatchAvgTime);
		logger.info("ThreadPoolExecutor started with " + activeStorageWriters + " active collectors.");

		// Register the event service with the coordination service
		clusterService.getRegistrationService().registerAsStorageWriter();
	}

	@Override
	public synchronized void start()
	{
		int checkForWorkInterval = props.get("storage_writer.check_for_work_every_x_secs", Defaults.STORAGE_WRITER_CHECK_FOR_WORK_EVERY_X_SECS);

		if (scheduledWriterService == null || scheduledWriterService.isShutdown())
		{
			scheduledWriterService = Executors.newScheduledThreadPool(1, new ThreadFactory()
			{
				public Thread newThread(Runnable runnable)
				{
					return new Thread(runnable, "StorageWriter");
				}
			});

			scheduledWriterService.scheduleAtFixedRate(new Runnable()
			{
				public void run()
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

								// Get a stat based on channel name.
								AvgStatOverTime channelStat = statsManager.createAvgAndCountStat(groupName, batchPeriodMapper.getChannelName() + ".AvgTime", batchPeriodMapper.getChannelName() + ".Count");
								batchWriter.setChannelStat(channelStat);

								threadPoolExecutor.submit(batchWriter);
							}
							catch (Exception e)
							{
								logger.error("StorageWriter error: {}|{}", batchPeriodMapper.getChannelName(), batchPeriodMapper.getNearestPeriodCeiling().toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")), e);
							}

							logger.debug("BatchMataData submitted for collection: {}|{}", batchPeriodMapper.getChannelName(), batchPeriodMapper.getNearestPeriodCeiling().toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")));
						}
					}
					else
					{
						logger.debug("Could not take on new work.  All threads busy. ActiveWriters={}", threadPoolExecutor.getActiveCount());
					}
				}
			}, 0, checkForWorkInterval, TimeUnit.SECONDS);

			logger.info("StorageWriter service has been started.");
		}
	}

	@Override
	public void shutdown()
	{
		clusterService.getRegistrationService().unRegisterAsStorageWriter();

		int waitForShutdown = props.get("storage_writer.wait_for_shutdown_mins", Defaults.STORAGE_WRITER_WAIT_FOR_SHUTDOWN_MINS);

		logger.info("ThreadPoolExecutor shutdown requested. " + threadPoolExecutor.getActiveCount() + " threads still active.");
		threadPoolExecutor.shutdown();

		try
		{
			logger.info("Waiting up to {} minutes to shutdown writer queue.", waitForShutdown);
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
			throw Throwables.propagate(e);
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
