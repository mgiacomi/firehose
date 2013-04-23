package com.gltech.scale.monitoring.services;

import com.gltech.scale.core.stats.results.AvgStat;
import com.gltech.scale.core.stats.results.GroupStats;
import com.gltech.scale.core.stats.results.OverTime;
import com.gltech.scale.monitoring.model.AggregateOverTime;
import com.gltech.scale.monitoring.model.ClusterStats;
import com.gltech.scale.monitoring.model.ResultsIO;
import com.gltech.scale.monitoring.model.ServerStats;
import com.google.inject.Inject;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;

public class ClusterStatsServiceImpl implements ClusterStatsService
{
	private ConcurrentMap<String, ServerStats> serverStatsMap = new ConcurrentHashMap<>();
	private Set<ClusterStatsCallBack> callBacks = new CopyOnWriteArraySet<>();
	private ResultsIO resultsIO;

	@Inject
	public ClusterStatsServiceImpl(ResultsIO resultsIO)
	{
		this.resultsIO = resultsIO;
	}

	@Override
	public void updateGroupStats(List<ServerStats> serverStatsList)
	{
		ConcurrentMap<String, ServerStats> newServerStatsMap = new ConcurrentHashMap<>();
		for (ServerStats serverStats : serverStatsList)
		{
			newServerStatsMap.put(serverStats.getWorkerId(), serverStats);
		}

		serverStatsMap = newServerStatsMap;

		for (ClusterStatsCallBack clusterStatsCallBack : callBacks)
		{
			clusterStatsCallBack.clusterStatsUpdate(getClusterStats(serverStatsList));
		}
	}

	@Override
	public String getJsonStatsAll()
	{
		List<ServerStats> sortedStatsList = new ArrayList<>(serverStatsMap.values());
		Collections.sort(sortedStatsList, new Comparator<ServerStats>()
		{
			public int compare(ServerStats o1, ServerStats o2)
			{
				return o1.getWorkerId().compareTo(o2.getWorkerId());
			}
		});

		return resultsIO.toJson(getClusterStats(sortedStatsList));
	}

	@Override
	public void registerCallback(ClusterStatsCallBack clusterStatsCallBack)
	{
		callBacks.add(clusterStatsCallBack);
	}

	@Override
	public void unRegisterCallback(ClusterStatsCallBack clusterStatsCallBack)
	{
		callBacks.remove(clusterStatsCallBack);
	}

	private ClusterStats getClusterStats(List<ServerStats> serverStatsList)
	{
		//Map<String, Map<String, AggregateOverTime>> aggregateStats = new HashMap<>();

		ClusterStats clusterStats = new ClusterStats();
		clusterStats.setStats(serverStatsList);

		for (ServerStats serverStats : serverStatsList)
		{
			// Aggregrate stats by group and name
			for (GroupStats groupStats : serverStats.getGroupStatsList())
			{
				// If the group doesn't exist create it
				if (!clusterStats.getAggregateStats().containsKey(groupStats.getName()))
				{
					clusterStats.getAggregateStats().put(groupStats.getName(), new HashMap<String, AggregateOverTime>());
				}

				Map<String, AggregateOverTime> aggregateOverTimeMap = clusterStats.getAggregateStats().get(groupStats.getName());

				genAvgCountHighLow(groupStats, aggregateOverTimeMap);
			}

			GroupStats commonGroup = null;

			for (GroupStats groupStats : serverStats.getGroupStatsList())
			{
				if ("common".equalsIgnoreCase(groupStats.getName()))
				{
					commonGroup = groupStats;
				}
			}


			// If the role group doesn't exist create it
			if (commonGroup != null)
			{
				for (String role : serverStats.getRoles())
				{
					String roleName = "role_" + role;
					if (!clusterStats.getAggregateStats().containsKey(roleName))
					{
						clusterStats.getAggregateStats().put(roleName, new HashMap<String, AggregateOverTime>());
					}

					Map<String, AggregateOverTime> aggregateOverTimeMap = clusterStats.getAggregateStats().get(roleName);

					genAvgCountHighLow(commonGroup, aggregateOverTimeMap);
				}
			}
		}

		return clusterStats;
	}

	private void genAvgCountHighLow(GroupStats groupStats, Map<String, AggregateOverTime> aggregateOverTimeMap)
	{
		if (groupStats.getAvgStats() != null)
		{
			for (OverTime<AvgStat> overTime : groupStats.getAvgStats())
			{
				String statName = overTime.getName().replace(".", "_");

				// If the stat doesn't exist create it.
				if (!aggregateOverTimeMap.containsKey(statName))
				{
					AggregateOverTime aggregateOverTime = new AggregateOverTime();
					aggregateOverTime.setName(statName);
					aggregateOverTime.setUnitOfMeasure(overTime.getUnitOfMeasure());
					aggregateOverTime.setAvgSec5(new AvgStat(overTime.getSec5().getTotal(), overTime.getSec5().getCount()));
					aggregateOverTime.setAvgMin1(new AvgStat(overTime.getMin1().getTotal(), overTime.getMin1().getCount()));
					aggregateOverTime.setAvgMin5(new AvgStat(overTime.getMin5().getTotal(), overTime.getMin5().getCount()));
					aggregateOverTime.setAvgMin30(new AvgStat(overTime.getMin30().getTotal(), overTime.getMin30().getCount()));
					aggregateOverTime.setAvgHour2(new AvgStat(overTime.getHour2().getTotal(), overTime.getHour2().getCount()));
					aggregateOverTime.setTotalSec5(overTime.getSec5().getAverage());
					aggregateOverTime.setTotalMin1(overTime.getMin1().getAverage());
					aggregateOverTime.setTotalMin5(overTime.getMin5().getAverage());
					aggregateOverTime.setTotalMin30(overTime.getMin30().getAverage());
					aggregateOverTime.setTotalHour2(overTime.getHour2().getAverage());
					aggregateOverTime.setHighSec5(overTime.getSec5().getAverage());
					aggregateOverTime.setHighMin1(overTime.getMin1().getAverage());
					aggregateOverTime.setHighMin5(overTime.getMin5().getAverage());
					aggregateOverTime.setHighMin30(overTime.getMin30().getAverage());
					aggregateOverTime.setHighHour2(overTime.getHour2().getAverage());
					aggregateOverTime.setLowSec5(overTime.getSec5().getAverage());
					aggregateOverTime.setLowMin1(overTime.getMin1().getAverage());
					aggregateOverTime.setLowMin5(overTime.getMin5().getAverage());
					aggregateOverTime.setLowMin30(overTime.getMin30().getAverage());
					aggregateOverTime.setLowHour2(overTime.getHour2().getAverage());
					aggregateOverTimeMap.put(statName, aggregateOverTime);
				}
				else
				{
					AggregateOverTime aggregateOverTime = aggregateOverTimeMap.get(statName);

					// Update Averages
					aggregateOverTime.setAvgSec5(new AvgStat(overTime.getSec5().getTotal() + aggregateOverTime.getAvgSec5().getTotal(), overTime.getSec5().getCount() + aggregateOverTime.getAvgSec5().getCount()));
					aggregateOverTime.setAvgMin1(new AvgStat(overTime.getMin1().getTotal() + aggregateOverTime.getAvgMin1().getTotal(), overTime.getMin1().getCount() + aggregateOverTime.getAvgMin1().getCount()));
					aggregateOverTime.setAvgMin5(new AvgStat(overTime.getMin5().getTotal() + aggregateOverTime.getAvgMin5().getTotal(), overTime.getMin5().getCount() + aggregateOverTime.getAvgMin5().getCount()));
					aggregateOverTime.setAvgMin30(new AvgStat(overTime.getMin30().getTotal() + aggregateOverTime.getAvgMin30().getTotal(), overTime.getMin30().getCount() + aggregateOverTime.getAvgMin30().getCount()));
					aggregateOverTime.setAvgHour2(new AvgStat(overTime.getHour2().getTotal() + aggregateOverTime.getAvgHour2().getTotal(), overTime.getHour2().getCount() + aggregateOverTime.getAvgHour2().getCount()));

					// Update Totals
					aggregateOverTime.setTotalSec5(overTime.getSec5().getAverage() + aggregateOverTime.getTotalSec5());
					aggregateOverTime.setTotalMin1(overTime.getMin1().getAverage() + aggregateOverTime.getTotalMin1());
					aggregateOverTime.setTotalMin5(overTime.getMin5().getAverage() + aggregateOverTime.getTotalMin5());
					aggregateOverTime.setTotalMin30(overTime.getMin30().getAverage() + aggregateOverTime.getTotalMin30());
					aggregateOverTime.setTotalHour2(overTime.getHour2().getAverage() + aggregateOverTime.getTotalHour2());

					// Update Highs
					if (overTime.getSec5().getAverage() > aggregateOverTime.getHighSec5())
					{
						aggregateOverTime.setHighSec5(overTime.getSec5().getAverage());
					}
					if (overTime.getMin1().getAverage() > aggregateOverTime.getHighMin1())
					{
						aggregateOverTime.setHighMin1(overTime.getMin1().getAverage());
					}
					if (overTime.getMin5().getAverage() > aggregateOverTime.getHighMin5())
					{
						aggregateOverTime.setHighMin5(overTime.getMin5().getAverage());
					}
					if (overTime.getMin30().getAverage() > aggregateOverTime.getHighMin30())
					{
						aggregateOverTime.setHighMin30(overTime.getMin30().getAverage());
					}
					if (overTime.getHour2().getAverage() > aggregateOverTime.getHighHour2())
					{
						aggregateOverTime.setHighHour2(overTime.getHour2().getAverage());
					}

					// Update Lows
					if (overTime.getSec5().getAverage() < aggregateOverTime.getLowSec5())
					{
						aggregateOverTime.setLowSec5(overTime.getSec5().getAverage());
					}
					if (overTime.getMin1().getAverage() < aggregateOverTime.getLowMin1())
					{
						aggregateOverTime.setLowMin1(overTime.getMin1().getAverage());
					}
					if (overTime.getMin5().getAverage() < aggregateOverTime.getLowMin5())
					{
						aggregateOverTime.setLowMin5(overTime.getMin5().getAverage());
					}
					if (overTime.getMin30().getAverage() < aggregateOverTime.getLowMin30())
					{
						aggregateOverTime.setLowMin30(overTime.getMin30().getAverage());
					}
					if (overTime.getHour2().getAverage() < aggregateOverTime.getLowHour2())
					{
						aggregateOverTime.setLowHour2(overTime.getHour2().getAverage());
					}
				}
			}
		}

		if (groupStats.getCountStats() != null)
		{
			for (OverTime<Long> overTime : groupStats.getCountStats())
			{
				String statName = overTime.getName().replace(".", "_");

				// If the stat doesn't exist create it.
				if (!aggregateOverTimeMap.containsKey(statName))
				{
					AggregateOverTime aggregateOverTime = new AggregateOverTime();
					aggregateOverTime.setName(statName);
					aggregateOverTime.setUnitOfMeasure(overTime.getUnitOfMeasure());
					aggregateOverTime.setAvgSec5(new AvgStat(overTime.getSec5(), 1));
					aggregateOverTime.setAvgMin1(new AvgStat(overTime.getMin1(), 1));
					aggregateOverTime.setAvgMin5(new AvgStat(overTime.getMin5(), 1));
					aggregateOverTime.setAvgMin30(new AvgStat(overTime.getMin30(), 1));
					aggregateOverTime.setAvgHour2(new AvgStat(overTime.getHour2(), 1));
					aggregateOverTime.setTotalSec5(overTime.getSec5());
					aggregateOverTime.setTotalMin1(overTime.getMin1());
					aggregateOverTime.setTotalMin5(overTime.getMin5());
					aggregateOverTime.setTotalMin30(overTime.getMin30());
					aggregateOverTime.setTotalHour2(overTime.getHour2());
					aggregateOverTime.setHighSec5(overTime.getSec5());
					aggregateOverTime.setHighMin1(overTime.getMin1());
					aggregateOverTime.setHighMin5(overTime.getMin5());
					aggregateOverTime.setHighMin30(overTime.getMin30());
					aggregateOverTime.setHighHour2(overTime.getHour2());
					aggregateOverTime.setLowSec5(overTime.getSec5());
					aggregateOverTime.setLowMin1(overTime.getMin1());
					aggregateOverTime.setLowMin5(overTime.getMin5());
					aggregateOverTime.setLowMin30(overTime.getMin30());
					aggregateOverTime.setLowHour2(overTime.getHour2());
					aggregateOverTimeMap.put(statName, aggregateOverTime);
				}
				else
				{
					AggregateOverTime aggregateOverTime = aggregateOverTimeMap.get(statName);

					// Update Averages
					aggregateOverTime.setAvgSec5(new AvgStat(overTime.getSec5() + aggregateOverTime.getAvgSec5().getTotal(), aggregateOverTime.getAvgSec5().getCount() + 1));
					aggregateOverTime.setAvgMin1(new AvgStat(overTime.getMin1() + aggregateOverTime.getAvgMin1().getTotal(), aggregateOverTime.getAvgMin1().getCount() + 1));
					aggregateOverTime.setAvgMin5(new AvgStat(overTime.getMin5() + aggregateOverTime.getAvgMin5().getTotal(), aggregateOverTime.getAvgMin5().getCount() + 1));
					aggregateOverTime.setAvgMin30(new AvgStat(overTime.getMin30() + aggregateOverTime.getAvgMin30().getTotal(), aggregateOverTime.getAvgMin30().getCount() + 1));
					aggregateOverTime.setAvgHour2(new AvgStat(overTime.getHour2() + aggregateOverTime.getAvgHour2().getTotal(), aggregateOverTime.getAvgHour2().getCount() + 1));

					// Update Totals
					aggregateOverTime.setTotalSec5(overTime.getSec5() + aggregateOverTime.getTotalSec5());
					aggregateOverTime.setTotalMin1(overTime.getMin1() + aggregateOverTime.getTotalMin1());
					aggregateOverTime.setTotalMin5(overTime.getMin5() + aggregateOverTime.getTotalMin5());
					aggregateOverTime.setTotalMin30(overTime.getMin30() + aggregateOverTime.getTotalMin30());
					aggregateOverTime.setTotalHour2(overTime.getHour2() + aggregateOverTime.getTotalHour2());

					// Update Highs
					if (overTime.getSec5() > aggregateOverTime.getHighSec5())
					{
						aggregateOverTime.setHighSec5(overTime.getSec5());
					}
					if (overTime.getMin1() > aggregateOverTime.getHighMin1())
					{
						aggregateOverTime.setHighMin1(overTime.getMin1());
					}
					if (overTime.getMin5() > aggregateOverTime.getHighMin5())
					{
						aggregateOverTime.setHighMin5(overTime.getMin5());
					}
					if (overTime.getMin30() > aggregateOverTime.getHighMin30())
					{
						aggregateOverTime.setHighMin30(overTime.getMin30());
					}
					if (overTime.getHour2() > aggregateOverTime.getHighHour2())
					{
						aggregateOverTime.setHighHour2(overTime.getHour2());
					}

					// Update Lows
					if (overTime.getSec5() < aggregateOverTime.getLowSec5())
					{
						aggregateOverTime.setLowSec5(overTime.getSec5());
					}
					if (overTime.getMin1() < aggregateOverTime.getLowMin1())
					{
						aggregateOverTime.setLowMin1(overTime.getMin1());
					}
					if (overTime.getMin5() < aggregateOverTime.getLowMin5())
					{
						aggregateOverTime.setLowMin5(overTime.getMin5());
					}
					if (overTime.getMin30() < aggregateOverTime.getLowMin30())
					{
						aggregateOverTime.setLowMin30(overTime.getMin30());
					}
					if (overTime.getHour2() < aggregateOverTime.getLowHour2())
					{
						aggregateOverTime.setLowHour2(overTime.getHour2());
					}
				}
			}
		}
	}

	private OverTime<AvgStat> getAvgStatByName(String groupName, String statName, ServerStats serverStats)
	{
		for (GroupStats groupStats : serverStats.getGroupStatsList())
		{
			if (groupStats.getName().equalsIgnoreCase(groupName))
			{
				for (OverTime<AvgStat> avgStat : groupStats.getAvgStats())
				{
					if (avgStat.getName().equalsIgnoreCase(statName))
					{
						return avgStat;
					}
				}
			}
		}

		return null;
	}
}
