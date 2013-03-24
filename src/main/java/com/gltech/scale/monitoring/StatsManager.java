package com.gltech.scale.monitoring;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.lifecycle.LifeCycle;
import com.gltech.scale.monitoring.results.AvgStat;
import com.gltech.scale.monitoring.results.GroupStats;
import com.gltech.scale.monitoring.results.OverTime;
import com.gltech.scale.util.Props;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class StatsManager implements Runnable, LifeCycle
{
	private static final Logger logger = LoggerFactory.getLogger(StatsManager.class);
	LinkedBuffer linkedBuffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
	private Schema<GroupStats> groupStatsSchema = RuntimeSchema.getSchema(GroupStats.class);
	private volatile boolean shutdown = false;
	Props props = Props.getProps();

	// Multi-level Map used to hold group and name level stat data.
	private static ConcurrentMap<String, ConcurrentMap<String, StatOverTime>> stats = new ConcurrentHashMap<>();

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

	public byte[] toBytes()
	{
		try
		{
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			ProtostuffIOUtil.writeListTo(out, getGroupStats(), groupStatsSchema, linkedBuffer);
			linkedBuffer.clear();
			return out.toByteArray();
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}

	public String toJson()
	{
		try
		{
			ObjectMapper mapper = new ObjectMapper();
			return mapper.writeValueAsString(getGroupStats());
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}

	public ArrayList<GroupStats> getGroupStats()
	{
		ArrayList<GroupStats> groupStatsList = new ArrayList<>();

		for (String groupName : stats.keySet())
		{
			GroupStats groupStats = new GroupStats(groupName);

			for (String statName : stats.get(groupName).keySet())
			{
				StatOverTime statOverTime = stats.get(groupName).get(statName);

				if (statOverTime instanceof AvgStatOverTime)
				{
					AvgStatOverTime avgStatOverTime = (AvgStatOverTime) statOverTime;

					OverTime<AvgStat> overTime = new OverTime<>(
							statName,
							avgStatOverTime.getAvgOverMinutes(1),
							avgStatOverTime.getAvgOverMinutes(5),
							avgStatOverTime.getAvgOverMinutes(30),
							avgStatOverTime.getAvgOverHours(2)
					);

					groupStats.getAvgStats().add(overTime);
				}
				if (statOverTime instanceof CounterStatOverTime)
				{
					CounterStatOverTime counterStatOverTime = (CounterStatOverTime) statOverTime;

					OverTime<Long> overTime = new OverTime<>(
							statName,
							counterStatOverTime.getCountOverMinutes(1),
							counterStatOverTime.getCountOverMinutes(5),
							counterStatOverTime.getCountOverMinutes(30),
							counterStatOverTime.getCountOverHours(2)
					);

					groupStats.getCountStats().add(overTime);
				}
			}

			groupStatsList.add(groupStats);
		}

		return groupStatsList;
	}

	@Override
	public void run()
	{
		logger.info("StatsManager starting.");

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
			logger.error("StatsManager was inturrupted.", e);
		}

		logger.info("StatsManager has been shutdown.");
	}

	@Override
	public void shutdown()
	{
		shutdown = true;
	}
}
