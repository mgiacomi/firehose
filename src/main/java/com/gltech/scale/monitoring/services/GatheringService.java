package com.gltech.scale.monitoring.services;

import com.gltech.scale.core.cluster.registration.RegistrationService;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.core.stats.StatsRestClient;
import com.gltech.scale.core.stats.results.ServerStats;
import com.gltech.scale.lifecycle.LifeCycle;
import com.gltech.scale.util.Props;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class GatheringService implements LifeCycle
{
	private static final Logger logger = LoggerFactory.getLogger(GatheringService.class);
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
					List<ServiceMetaData> servers = registrationService.getRegisteredServers();

					for (ServiceMetaData serviceMetaData : servers)
					{
						try
						{
							ServerStats serverStats = statsRestClient.getServerStats(serviceMetaData);
							clusterStatsService.updateGroupStats(serverStats);
						}
						catch (Exception e)
						{
							logger.error("Failed to gather stats for server {}", serviceMetaData, e);
						}
					}

					logger.debug("Stats have been updated for {} servers", servers.size());
				}
			}, runEveryXSeconds, runEveryXSeconds, TimeUnit.SECONDS);

			logger.info("GatheringService call back service has been started.");
		}
	}

	@Override
	public void shutdown()
	{
		scheduledGatherService.shutdown();
		logger.info("StatsManager has been shutdown.");
	}

}
