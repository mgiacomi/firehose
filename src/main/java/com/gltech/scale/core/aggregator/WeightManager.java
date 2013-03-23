package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.cluster.ChannelCoordinator;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.util.Props;
import com.google.inject.Inject;
import com.gltech.scale.lifecycle.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WeightManager implements Runnable, LifeCycle
{
	private static final Logger logger = LoggerFactory.getLogger(WeightManager.class);
	private final Aggregator aggregator;
	private final ChannelCoordinator channelCoordinator;
	private volatile boolean shutdown = false;
	Props props = Props.getProps();

	@Inject
	public WeightManager(Aggregator aggregator, ChannelCoordinator channelCoordinator)
	{
		this.aggregator = aggregator;
		this.channelCoordinator = channelCoordinator;
	}

	@Override
	public void run()
	{
		try
		{
			while (!shutdown)
			{
				try
				{
					boolean active = true;
					int primaries = aggregator.getActiveBatches().size();
					int backups = aggregator.getActiveBackupBatches().size();
					int rested = 999;

					if (primaries == 0 && backups == 0)
					{
						active = false;
						rested--;
					}

					channelCoordinator.registerWeight(active, primaries, backups, rested);
					logger.trace("Registering weight with ChannelCoordinator. active={}, primaries={}, backups={}, rested={}", active, primaries, backups, rested);
				}
				catch (Exception e)
				{
					// May fail due to network/zookeeper issues.  If so, just try again next time.
					logger.error("Failed to update weight for Aggregator.", e);
				}

				Thread.sleep(props.get("aggregator.weight_manager_sleep_millis", Defaults.WEIGHT_MANGER_SLEEP_MILLIS));
			}
		}
		catch (InterruptedException e)
		{
			logger.error("WeightManager was inturrupted.", e);
		}

		logger.info("WeightManager has been shutdown.");
	}

	public void shutdown()
	{
		shutdown = true;
	}
}
