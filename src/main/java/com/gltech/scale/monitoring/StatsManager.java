package com.gltech.scale.monitoring;

import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.lifecycle.LifeCycle;
import com.gltech.scale.util.Props;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class StatsManager implements Runnable, LifeCycle
{
	private static final Logger logger = LoggerFactory.getLogger(StatsManager.class);
	private volatile boolean shutdown = false;
	Props props = Props.getProps();

	// Multi-level Map used to hold group and name level stat data.
	private ConcurrentMap<String, ConcurrentMap<String, StatOverTime>> stats = new ConcurrentHashMap<>();

	// Factory method to make sure that new stats are registered
	public AvgStatOverTime createAvgStat(String groupName, String statName)
	{
		AvgStatOverTime avgStatOverTime = new AvgStatOverTime();
		StatOverTime statOverTime = registerStat(groupName, statName, avgStatOverTime);

		try
		{
			return (AvgStatOverTime) statOverTime;
		}
		catch (ClassCastException e)
		{
			throw new IllegalStateException("A stat has already been created with the same name, but of a different type. {name=" + statName + ", type=" + statOverTime.getClass().getSimpleName() + "}");
		}
	}

	// Factory method to make sure that new stats are registered
	public CounterStatOverTime createCounterStat(String groupName, String statName)
	{
		CounterStatOverTime counterStatOverTime = new CounterStatOverTime();
		StatOverTime statOverTime = registerStat(groupName, statName, counterStatOverTime);

		try
		{
			return (CounterStatOverTime) statOverTime;
		}
		catch (ClassCastException e)
		{
			throw new IllegalStateException("A stat has already been created with the same name, but of a different type. {name=" + statName + ", type=" + statOverTime.getClass().getSimpleName() + "}");
		}
	}

	private StatOverTime registerStat(String groupName, String statName, StatOverTime statOverTime)
	{
		ConcurrentMap<String, StatOverTime> groupMap = stats.get(groupName);

		if (groupMap == null)
		{
			ConcurrentMap<String, StatOverTime> newGroupMap = new ConcurrentHashMap<>();
			groupMap = stats.putIfAbsent(groupName, newGroupMap);
			if (groupMap == null)
			{
				groupMap = newGroupMap;
			}
		}

		StatOverTime existingStatOverTime = groupMap.putIfAbsent(statName, statOverTime);

		if (existingStatOverTime != null)
		{
			return existingStatOverTime;
		}

		return statOverTime;
	}

	@Override
	public void run()
	{
		try
		{
			while (!shutdown)
			{
				for (ConcurrentMap<String, StatOverTime> groupStats : stats.values())
				{
					for (StatOverTime statName : groupStats.values())
					{
						statName.cleanOldThanTwoHours();
					}
				}
			}

			int sleepInMinutes = props.get("monitoring.stats_manager_cleanup_sleep_minutes", Defaults.STATS_MANAGER_CLEANUP_SLEEP_MINS);
			TimeUnit.MINUTES.sleep(sleepInMinutes);
		}
		catch (InterruptedException e)
		{
			logger.error("WeightManager was inturrupted.", e);
		}

		logger.info("WeightManager has been shutdown.");
	}

	@Override
	public void shutdown()
	{
		shutdown = true;
	}
}
