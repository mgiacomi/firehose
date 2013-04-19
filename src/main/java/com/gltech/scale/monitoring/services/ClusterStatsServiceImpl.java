package com.gltech.scale.monitoring.services;

import com.gltech.scale.core.stats.results.AvgStat;
import com.gltech.scale.core.stats.results.GroupStats;
import com.gltech.scale.core.stats.results.OverTime;
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
		ClusterStats clusterStats = new ClusterStats();
		clusterStats.setStats(serverStatsList);

/*
		int aggregatorMsgInQue;
		int aggregatorQueSize;
		int aggregatorQueAge;
		int storageWriterMsgPerSec;
		int storageWriterBytesPerSec;
		int storageWriterBatchesBeingWritten;
		int outboundMsgPerSec;
		int outboundAvgMsgSize;
		int outboundActiveQueries;

Inbound
Aggregator
StorageWriter
*/
		Averager inboundLoad = new Averager();
		Averager inboundAvgMsgSize = new Averager();
		int inboundMsgPerSec = 0;
		int aggregatorMsgInQue = 0;
		int aggregatorQueSize = 0;
		int aggregatorQueAge = 0;

		for (ServerStats serverStats : serverStatsList)
		{
			if(serverStats.getRoles().contains("Inbound")) {
				OverTime<AvgStat> loadStat = getAvgStatByName("System", "LoadAvg", serverStats);
				if(loadStat != null) {
					inboundLoad.add(loadStat.getSec5().getAverage());
				}

				OverTime<AvgStat> avgMsgSize = getAvgStatByName("Inbound", "AddMessage.Size", serverStats);
				if(avgMsgSize != null) {
					inboundAvgMsgSize.add(avgMsgSize.getSec5().getAverage());
				}

				OverTime<Long> msgPerSec = getCountStatByName("Inbound", "AddMessage.Count", serverStats);
				if(msgPerSec != null) {
					inboundMsgPerSec += msgPerSec.getSec5();
				}

				OverTime<Long> queSize = getCountStatByName("Aggregator", "TotalQueueSize.Avg", serverStats);
				if(queSize != null) {
					aggregatorQueSize += queSize.getSec5();
				}

				OverTime<Long> msgInQue = getCountStatByName("Aggregator", "MessagesInQueue.Avg", serverStats);
				if(msgInQue != null) {
					aggregatorMsgInQue += msgInQue.getSec5();
				}

				OverTime<AvgStat> queAge = getAvgStatByName("Aggregator", "OldestInQueue.Avg", serverStats);
				if(queAge != null) {
					if(queAge.getSec5().getAverage() > aggregatorQueAge)
					{
						aggregatorQueAge = Math.round(queAge.getSec5().getAverage());
					}
				}

/*
				OverTime<AvgStat> xxx = getAvgStatByName("xxx", "xxx", serverStats);
				if(xxx != null) {
					inboundxxx.add(xxx.getSec5().getAverage());
				}
				OverTime<AvgStat> xxx = getAvgStatByName("xxx", "xxx", serverStats);
				if(xxx != null) {
					inboundxxx.add(xxx.getSec5().getAverage());
				}
				OverTime<AvgStat> xxx = getAvgStatByName("xxx", "xxx", serverStats);
				if(xxx != null) {
					inboundxxx.add(xxx.getSec5().getAverage());
				}
				OverTime<AvgStat> xxx = getAvgStatByName("xxx", "xxx", serverStats);
				if(xxx != null) {
					inboundxxx.add(xxx.getSec5().getAverage());
				}
				OverTime<AvgStat> xxx = getAvgStatByName("xxx", "xxx", serverStats);
				if(xxx != null) {
					inboundxxx.add(xxx.getSec5().getAverage());
				}
*/
			}

		}

		// Inbound
		clusterStats.getAggregateStats().setInboundLoad(inboundLoad.getAvg());
		clusterStats.getAggregateStats().setInboundAvgMsgSize(inboundAvgMsgSize.getAvg());
		if(inboundMsgPerSec > 0)
		{
			clusterStats.getAggregateStats().setInboundMsgPerSec(inboundMsgPerSec / 5);
		}

		// Aggregator
		clusterStats.getAggregateStats().setAggregatorMsgInQue(aggregatorMsgInQue);
		clusterStats.getAggregateStats().setAggregatorQueAge(aggregatorQueAge);
		clusterStats.getAggregateStats().setAggregatorQueSize(aggregatorQueSize);

		return clusterStats;
	}

	private OverTime<AvgStat> getAvgStatByName(String groupName, String statName, ServerStats serverStats)
	{
		for(GroupStats groupStats : serverStats.getGroupStatsList())
		{
			if(groupStats.getName().equalsIgnoreCase(groupName))
			{
				for(OverTime<AvgStat> avgStat : groupStats.getAvgStats())
				{
					if(avgStat.getName().equalsIgnoreCase(statName))
					{
						return avgStat;
					}
				}
			}
		}

		return null;
	}

	private OverTime<Long> getCountStatByName(String groupName, String statName, ServerStats serverStats)
	{
		for(GroupStats groupStats : serverStats.getGroupStatsList())
		{
			if(groupStats.getName().equalsIgnoreCase(groupName))
			{
				for(OverTime<Long> countStat : groupStats.getCountStats())
				{
					if(countStat.getName().equalsIgnoreCase(statName))
					{
						return countStat;
					}
				}
			}
		}

		return null;
	}

	private class Averager
	{
		private int count;
		private float total;

		public void add(float increment)
		{
			total += increment;
			count++;
		}

		public int getAvg()
		{
			return Math.round(total / count);
		}
	}
}
