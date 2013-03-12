package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.cluster.ChannelCoordinator;
import com.google.inject.Inject;
import com.gltech.scale.lifecycle.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WeightManager implements Runnable, LifeCycle
{
	private static final Logger logger = LoggerFactory.getLogger("com.lokiscale.rope.WeightManager");
	private final Aggregator aggregator;
	private final ChannelCoordinator channelCoordinator;
	private volatile boolean shutdown = false;

	@Inject
	public WeightManager(Aggregator aggregator, ChannelCoordinator channelCoordinator)
	{
		this.aggregator = aggregator;
		this.channelCoordinator = channelCoordinator;
	}

	public void run()
	{
		try
		{
			while (!shutdown)
			{
				try
				{
					boolean active = true;
					int primaries = aggregator.getActiveTimeBuckets().size();
					int backups = aggregator.getActiveBackupTimeBuckets().size();
					int rested = 999;

					if (primaries == 0 && backups == 0)
					{
						active = false;
						rested--;
					}

					channelCoordinator.registerWeight(active, primaries, backups, rested);
					logger.trace("Registering weight with RopeCoordinator. active={}, primaries={}, backups={}, rested={}", active, primaries, backups, rested);
				}
				catch (Exception e)
				{
					// May fail due to network/zookeeper issues.  If so, just try again next time.
					logger.error("Failed to update weight for RopeManager.", e);
				}

				Thread.sleep(500);
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
