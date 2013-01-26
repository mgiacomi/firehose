package com.gltech.scale.core.lifecycle;

import com.gltech.scale.core.util.Props;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Use the LifeCycleService as a delegate to pick up it's LifeCycle implementation.
 */
public class LifeCycleService implements LifeCycle
{
	private static final Logger logger = LoggerFactory.getLogger(LifeCycleService.class);

	private String name;
	private ExecutorService executorService;
	private List<Future> futures;

	public LifeCycleService(String name, ExecutorService executorService)
	{
		this(name, executorService, new ArrayList<Future>());
	}

	public LifeCycleService(String name, ExecutorService executorService, List<Future> futures)
	{
		this.name = name;
		this.executorService = executorService;
		this.futures = futures;
	}

	public boolean isShutdown()
	{
		return executorService.isShutdown();
	}

	public void shutdown()
	{
		Props props = Props.getProps();
		logger.info("shutting down " + name);
		try
		{
			executorService.shutdown();
			int shutdownSeconds = props.get("LIFE_CYCLE_SHUTDOWN_SECONDS", 60);
			logger.info("awaitTermination " + name + " for " + shutdownSeconds + " seconds");
			if (!executorService.awaitTermination(shutdownSeconds, TimeUnit.SECONDS))
			{
				logger.info("shutdownNow " + name);
				executorService.shutdownNow();
				int shutdownNowSeconds = props.get("LIFE_CYCLE_SHUTDOWN_NOW_SECONDS", 60);
				logger.info("awaitTermination again " + name + " for " + shutdownNowSeconds + " seconds");
				if (!executorService.awaitTermination(shutdownNowSeconds, TimeUnit.SECONDS))
				{
					logger.error("unable to cancel all processes " + name);
					for (Future future : futures)
					{
						if (future.isDone() || future.isCancelled())
						{
							logger.info("completed " + name + " " + future);
						}
						else
						{
							logger.info("still running " + name + " " + future);
						}
					}
				}
			}
		}
		catch (InterruptedException ie)
		{
			logger.warn("InterruptedException " + name);
			executorService.shutdownNow();
			Thread.currentThread().interrupt();
		}
		logger.info("completed shutdown " + name);
	}


	public String toString()
	{
		return "LifeCycleService{" +
				"name='" + name + '\'' +
				'}';
	}
}
