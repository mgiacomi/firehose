package com.gltech.scale.core.rope;

import com.gltech.scale.core.cluster.ChannelCoordinator;
import com.google.inject.Inject;
import com.gltech.scale.core.lifecycle.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WeightManager implements Runnable, LifeCycle
{
	private static final Logger logger = LoggerFactory.getLogger("com.lokiscale.rope.WeightManager");
	private final RopeManager ropeManager;
	private final ChannelCoordinator channelCoordinator;
	private volatile boolean shutdown = false;

	@Inject
	public WeightManager(RopeManager ropeManager, ChannelCoordinator channelCoordinator)
	{
		this.ropeManager = ropeManager;
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
					int primaries = ropeManager.getActiveTimeBuckets().size();
					int backups = ropeManager.getActiveBackupTimeBuckets().size();
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
