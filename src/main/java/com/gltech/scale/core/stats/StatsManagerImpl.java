package com.gltech.scale.core.stats;

import com.gltech.scale.core.cluster.registration.RegistrationService;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.core.stats.results.*;
import com.gltech.scale.monitoring.model.ServerStats;
import com.gltech.scale.util.Props;
import com.google.inject.Inject;
import ganglia.gmetric.GMetric;
import ganglia.gmetric.GMetricSlope;
import ganglia.gmetric.GMetricType;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

public class StatsManagerImpl implements StatsManager
{
	private static final Logger logger = LoggerFactory.getLogger(StatsManagerImpl.class);
	private static ScheduledExecutorService scheduledCleanUpService;
	private static ScheduledExecutorService scheduledCallBackService;
	private static ScheduledExecutorService scheduledPublishService;
	private Props props = Props.getProps();
	private RegistrationService registrationService;

	// Multi-level Map used to hold group and name level stat data.
	private static final ConcurrentMap<String, ConcurrentMap<String, StatOverTime>> stats = new ConcurrentHashMap<>();

	// List of callbacks that need to be returned into their associated stat
	private static final ConcurrentMap<StatOverTime, StatCallBack> callbacks = new ConcurrentHashMap<>();

	@Inject
	public StatsManagerImpl(RegistrationService registrationService)
	{
		this.registrationService = registrationService;
	}

	@Override
	public synchronized void start()
	{
		if (scheduledCleanUpService == null || scheduledCleanUpService.isShutdown())
		{
			scheduledCleanUpService = Executors.newScheduledThreadPool(1, new ThreadFactory()
			{
				public Thread newThread(Runnable runnable)
				{
					return new Thread(runnable, "StatsManagerCleanUp");
				}
			});

			int runEveryXMinutes = props.get("monitoring.stats_manager_cleanup_run_every_x_minutes", Defaults.STATS_MANAGER_CLEANUP_RUN_EVERY_X_MINS);

			scheduledCleanUpService.scheduleAtFixedRate(new Runnable()
			{
				public void run()
				{
					for (ConcurrentMap<String, StatOverTime> groupStats : stats.values())
					{
						for (StatOverTime statName : groupStats.values())
						{
							statName.cleanOldThanTwoHours();
						}
					}
				}
			}, 0, runEveryXMinutes, TimeUnit.MINUTES);

			logger.info("StatsManager clean up service has been started.");
		}

		if (scheduledCallBackService == null || scheduledCallBackService.isShutdown())
		{
			scheduledCallBackService = Executors.newScheduledThreadPool(1, new ThreadFactory()
			{
				public Thread newThread(Runnable runnable)
				{
					return new Thread(runnable, "StatsManagerCallBack");
				}
			});

			int runEveryXSeconds = props.get("monitoring.stats_manager_callback_run_every_x_seconds", Defaults.STATS_MANAGER_CALLBACK_RUN_EVERY_X_SECONDS);

			scheduledCallBackService.scheduleAtFixedRate(new Runnable()
			{
				public void run()
				{
					// Loop through each callback and update the stats
					for (StatOverTime statOverTime : callbacks.keySet())
					{
						statOverTime.add(callbacks.get(statOverTime).getValue());
					}
				}
			}, runEveryXSeconds, runEveryXSeconds, TimeUnit.SECONDS);

			logger.info("StatsManager call back service has been started.");
		}

		scheduledPublishService = Executors.newScheduledThreadPool(1, new ThreadFactory()
		{
			public Thread newThread(Runnable runnable)
			{
				return new Thread(runnable, "MonitoringPublisher");
			}
		});
		scheduledPublishService.scheduleAtFixedRate(new Runnable()
		{
			public void run()
			{
				Props props = Props.getProps();
				String ipAddress = props.get("gangliaIpaddress", "localhost");
				int port = props.get("gangliaPort", 8649);
				GMetric gMetric = new GMetric(ipAddress, port, GMetric.UDPAddressingMode.UNICAST, true);

				for (GroupStats groupStats : getServerStats().getGroupStatsList())
				{
					for (OverTime<AvgStat> avgStat : groupStats.getAvgStats())
					{
						try
						{
							logger.debug("publishing " + avgStat.getName() + " " + avgStat.getMin1().getAverage() + " : " + avgStat.getUnitOfMeasure());
							gMetric.announce(avgStat.getName(), String.valueOf(avgStat.getMin1().getAverage()), GMetricType.DOUBLE, avgStat.getUnitOfMeasure(), GMetricSlope.BOTH, 60, 1440 * 60, groupStats.getName());
						}
						catch (Exception e)
						{
							logger.warn("unable to publish GMetric: " + avgStat.getName(), e);
						}
					}
					for (OverTime<Long> countStat : groupStats.getCountStats())
					{
						try
						{
							logger.debug("publishing " + countStat.getName() + " " + countStat.getMin1() + " : " + countStat.getUnitOfMeasure());
							gMetric.announce(countStat.getName(), String.valueOf(countStat.getMin1()), GMetricType.DOUBLE, countStat.getUnitOfMeasure(), GMetricSlope.BOTH, 60, 1440 * 60, groupStats.getName());
						}
						catch (Exception e)
						{
							logger.warn("unable to publish GMetric: " + countStat.getName(), e);
						}
					}
				}
			}
		}, 60, 60, TimeUnit.SECONDS);
	}

	@Override
	public void shutdown()
	{
		scheduledCleanUpService.shutdown();
		scheduledCallBackService.shutdown();
		scheduledPublishService.shutdown();
		logger.info("StatsManager has been shutdown.");
	}

	@Override
	public AvgStatOverTime createAvgStat(String groupName, String avgStatName, String unitOfMeasure)
	{
		return createAvgStat(groupName, avgStatName, unitOfMeasure, null);
	}

	@Override
	public AvgStatOverTime createAvgStat(String groupName, String avgStatName, String unitOfMeasure, StatCallBack statCallBack)
	{
		AvgStatOverTime avgStatOverTime = new AvgStatOverTime(avgStatName, unitOfMeasure);
		StatOverTime statOverTime = registerStat(groupName, avgStatName, avgStatOverTime, statCallBack);

		try
		{
			return (AvgStatOverTime) statOverTime;
		}
		catch (ClassCastException e)
		{
			throw new IllegalStateException("A stat has already been created with the same name, but of a different type. {name=" + avgStatName + ", type=" + statOverTime.getClass().getSimpleName() + "}");
		}
	}

	@Override
	public CounterStatOverTime createCounterStat(String groupName, String statName, String unitOfMeasure)
	{
		return createCounterStat(groupName, statName, unitOfMeasure, null);
	}

	@Override
	public CounterStatOverTime createCounterStat(String groupName, String statName, String unitOfMeasure, StatCallBack statCallBack)
	{
		CounterStatOverTime counterStatOverTime = new CounterStatOverTime(statName, unitOfMeasure);
		StatOverTime statOverTime = registerStat(groupName, statName, counterStatOverTime, statCallBack);

		try
		{
			return (CounterStatOverTime) statOverTime;
		}
		catch (ClassCastException e)
		{
			throw new IllegalStateException("A stat has already been created with the same name, but of a different type. {name=" + statName + ", type=" + statOverTime.getClass().getSimpleName() + "}");
		}
	}

	private StatOverTime registerStat(String groupName, String statName, StatOverTime statOverTime, StatCallBack statCallBack)
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

		if (statCallBack != null)
		{
			callbacks.put(statOverTime, statCallBack);
		}

		StatOverTime existingStatOverTime = groupMap.putIfAbsent(statName, statOverTime);

		if (existingStatOverTime != null)
		{
			return existingStatOverTime;
		}

		return statOverTime;
	}

	@Override
	public ServerStats getServerStats()
	{
		Set<GroupStats> groupStatsList = new HashSet<>();

		for (String groupName : stats.keySet())
		{
			GroupStats groupStats = new GroupStats(groupName);

			for (String statName : stats.get(groupName).keySet())
			{
				StatOverTime statOverTime = stats.get(groupName).get(statName);

				if (statOverTime instanceof AvgStatOverTime)
				{
					AvgStatOverTime avgStatOverTime = (AvgStatOverTime) statOverTime;

					OverTime<AvgStat> avgOverTime = new OverTime<>(
							avgStatOverTime.getName(),
							avgStatOverTime.getUnitOfMeasure(),
							avgStatOverTime.getAvgOverMinutes(1),
							avgStatOverTime.getAvgOverMinutes(5),
							avgStatOverTime.getAvgOverMinutes(30),
							avgStatOverTime.getAvgOverHours(2)
					);

					groupStats.getAvgStats().add(avgOverTime);

					if (avgStatOverTime.getCounterStatOverTime().getName() != null)
					{
						OverTime<Long> countOverTime = new OverTime<>(
								avgStatOverTime.getCounterStatOverTime().getName(),
								avgStatOverTime.getCounterStatOverTime().getUnitOfMeasure(),
								avgStatOverTime.getCounterStatOverTime().getCountOverMinutes(1),
								avgStatOverTime.getCounterStatOverTime().getCountOverMinutes(5),
								avgStatOverTime.getCounterStatOverTime().getCountOverMinutes(30),
								avgStatOverTime.getCounterStatOverTime().getCountOverHours(2)
						);

						groupStats.getCountStats().add(countOverTime);
					}
				}
				if (statOverTime instanceof CounterStatOverTime)
				{
					CounterStatOverTime counterStatOverTime = (CounterStatOverTime) statOverTime;

					OverTime<Long> countOverTime = new OverTime<>(
							counterStatOverTime.getName(),
							counterStatOverTime.getUnitOfMeasure(),
							counterStatOverTime.getCountOverMinutes(1),
							counterStatOverTime.getCountOverMinutes(5),
							counterStatOverTime.getCountOverMinutes(30),
							counterStatOverTime.getCountOverHours(2)
					);

					groupStats.getCountStats().add(countOverTime);
				}
			}

			groupStatsList.add(groupStats);
		}

		ServerStats serverStats = new ServerStats();
		serverStats.setWorkerId(registrationService.getLocalServerMetaData().getWorkerId().toString());
		serverStats.setRoles(registrationService.getRoles());
		serverStats.setJoinDate(registrationService.getLocalServerRegistrationTime().toString(ISODateTimeFormat.dateTime()));
		serverStats.setStatus("Running");

		try
		{
			serverStats.setHostname(InetAddress.getLocalHost().getHostName());
		}
		catch (UnknownHostException e)
		{
			serverStats.setHostname("unknown");
		}

		serverStats.setGroupStatsList(groupStatsList);

		return serverStats;
	}
}
