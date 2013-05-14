package com.gltech.scale.monitoring.services;

import com.gltech.scale.core.cluster.registration.RegistrationService;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.core.stats.StatsRestClient;
import com.gltech.scale.monitoring.model.ServerStats;
import com.gltech.scale.lifecycle.LifeCycle;
import com.gltech.scale.util.Props;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

public class GatheringService implements LifeCycle
{
	private static final Logger logger = LoggerFactory.getLogger(GatheringService.class);
	private static ThreadPoolExecutor serverCollectorExecutor;
	private static ScheduledExecutorService scheduledGatherService;
	private RegistrationService registrationService;
	private ClusterStatsService clusterStatsService;
	private StatsRestClient statsRestClient;
	Props props = Props.getProps();

	@Inject
	public GatheringService(RegistrationService registrationService, ClusterStatsService clusterStatsService, StatsRestClient statsRestClient)
	{
		this.registrationService = registrationService;
		this.clusterStatsService = clusterStatsService;
		this.statsRestClient = statsRestClient;
	}

	public synchronized void start()
	{
		if (serverCollectorExecutor == null || serverCollectorExecutor.isShutdown())
		{
			TransferQueue<Runnable> queue = new LinkedTransferQueue<>();
			serverCollectorExecutor = new ThreadPoolExecutor(100, 100, 1, TimeUnit.HOURS, queue, new ThreadFactory()
			{
				public Thread newThread(Runnable r)
				{
					return new Thread(r, "ServerCollectorExecutor");
				}
			});

			logger.info("ServerCollectorExecutor has been started.");
		}

		if (scheduledGatherService == null || scheduledGatherService.isShutdown())
		{
			scheduledGatherService = Executors.newScheduledThreadPool(1, new ThreadFactory()
			{
				public Thread newThread(Runnable runnable)
				{
					return new Thread(runnable, "GatherService");
				}
			});

			int runEveryXSeconds = props.get("monitoring.gather_service_run_every_x_seconds", Defaults.GATHER_SERVICE_RUN_EVERY_X_SECONDS);

			scheduledGatherService.scheduleAtFixedRate(new Runnable()
			{
				public void run()
				{
					List<Callable<ServerStats>> callables = new ArrayList<>();

					for (final ServiceMetaData serviceMetaData : registrationService.getRegisteredServers())
					{
						callables.add(new Callable<ServerStats>()
						{
							public ServerStats call() throws Exception
							{
								return statsRestClient.getServerStats(serviceMetaData);
							}
						});
					}

					try
					{
						Collection<Future<ServerStats>> futures = serverCollectorExecutor.invokeAll(callables);

						List<ServerStats> serverStatsList = new ArrayList<>();

						for (Future<ServerStats> future : futures)
						{
							try
							{
								serverStatsList.add(future.get());
							}
							catch (Exception e)
							{
								logger.error("Failed to gather stats for server.", e);
							}
						}
						clusterStatsService.updateGroupStats(serverStatsList);

						logger.debug("Stats have been updated for {} of {} servers", serverStatsList.size(), futures.size());
					}
					catch (InterruptedException e)
					{
						logger.error("ServerCollectorExecutor was interrupted.", e);
					}
				}
			}, runEveryXSeconds, runEveryXSeconds, TimeUnit.SECONDS);

			logger.info("GatheringService call back service has been started.");
		}
	}

	@Override
	public void shutdown()
	{
		serverCollectorExecutor.shutdownNow();
		scheduledGatherService.shutdownNow();
		logger.info("StatsManager has been shutdown.");
	}

}
