package com.gltech.scale.lifecycle;

import com.google.common.collect.ArrayListMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Use the LifeCycleManager to register your LifeCycle implementation.
 * It registers itself for the shutdown hook when you register LifeCycles.
 * It will shutdown the registered LifeCycles in order.
 */
public class LifeCycleManager
{
	private static final Logger logger = LoggerFactory.getLogger(LifeCycleManager.class);

	private static final LifeCycleManager instance = new LifeCycleManager();
	private boolean initialized = false;
	private boolean shutdownComplete = false;

	private LifeCycleManager()
	{
	}

	public static LifeCycleManager getInstance()
	{
		return instance;
	}

	private final ArrayListMultimap<LifeCycle.Priority, LifeCycle> lifeCycles = ArrayListMultimap.create();

	public synchronized void add(LifeCycle lifeCycle, LifeCycle.Priority priority)
	{
		if (!initialized)
		{
			addShutdownHook();
			initialized = true;
		}
		logger.info("adding " + lifeCycle + " at priority " + priority);
		lifeCycles.put(priority, lifeCycle);
		shutdownComplete = false;
	}

	public void shutdown()
	{
		if (!shutdownComplete)
		{
			logger.info("COMMENCE SHUTDOWN ");
			shutdown(LifeCycle.Priority.INITIAL);
			shutdown(LifeCycle.Priority.SECOND);
			shutdown(LifeCycle.Priority.THIRD);
			shutdown(LifeCycle.Priority.FINAL);
			logger.info("COMPLETED SHUTDOWN ");
			shutdownComplete = true;
		}
		else
		{
			logger.info("LifeCycleManager has already been shutdown.");
		}
	}

	private void shutdown(LifeCycle.Priority priority)
	{
		logger.info("shutting down " + priority);
		//these could be made simultaneous
		Collection<LifeCycle> collection = lifeCycles.get(priority);
		for (LifeCycle lifeCycle : collection)
		{
			logger.info("shutting down " + lifeCycle);
			try
			{
				lifeCycle.shutdown();
			}
			catch (Exception e)
			{
				logger.error("unable to properly shut down " + lifeCycle, e);
			}
		}
	}

	private void addShutdownHook()
	{
		logger.info("adding shutdown hook");
		Runtime.getRuntime().addShutdownHook(new Thread()
		{
			public void run()
			{
				logger.warn("running shutdown hook");
				LifeCycleManager.getInstance().shutdown();
			}
		});

	}

}
