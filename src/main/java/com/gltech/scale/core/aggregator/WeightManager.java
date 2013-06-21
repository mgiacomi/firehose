package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.cluster.ChannelCoordinator;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.util.Props;
import com.google.inject.Inject;
import com.gltech.scale.lifecycle.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class WeightManager implements LifeCycle
{
	private static final Logger logger = LoggerFactory.getLogger(WeightManager.class);
	private final Aggregator aggregator;
	private final ChannelCoordinator channelCoordinator;
	private static ScheduledExecutorService scheduledUpdateWeightsService;
	Props props = Props.getProps();

	@Inject
	public WeightManager(Aggregator aggregator, ChannelCoordinator channelCoordinator)
	{
		this.aggregator = aggregator;
		this.channelCoordinator = channelCoordinator;
	}

	public synchronized void start()
	{
		if (scheduledUpdateWeightsService == null || scheduledUpdateWeightsService.isShutdown())
		{
			scheduledUpdateWeightsService = Executors.newScheduledThreadPool(1, new ThreadFactory()
			{
				public Thread newThread(Runnable runnable)
				{
					return new Thread(runnable, "WeightManager");
				}
			});

			int runEveryXMillis = props.get("aggregator.weight_manager_register_every_x_millis", Defaults.WEIGHT_MANGER_REGISTER_EVERY_X_MILLIS);

			scheduledUpdateWeightsService.scheduleAtFixedRate(new Runnable()
			{
				public void run()
				{
					try
					{
						boolean active = true;
						int primaries = aggregator.getActiveBatches().size();
						int backups = aggregator.getActiveBackupBatches().size();
						int atRest = 999;

						if (primaries == 0 && backups == 0)
						{
							active = false;
							atRest--;
						}

						channelCoordinator.registerWeight(active, primaries, backups, atRest);
						logger.trace("Registering weight with ChannelCoordinator. active={}, primaries={}, backups={}, atRest={}", active, primaries, backups, atRest);
					}
					catch (Exception e)
					{
						logger.error("ScheduledUpdateWeightsService failed.", e);
					}
				}
			}, 0, runEveryXMillis, TimeUnit.MILLISECONDS);

			logger.info("WeightManager has been started.");
		}
	}

	@Override
	public void shutdown()
	{
		scheduledUpdateWeightsService.shutdown();
		logger.info("WeightManager has been shutdown.");
	}
}
