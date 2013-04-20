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

		Averager inboundLoad = new Averager();
		Averager inboundAvgMsgSize = new Averager();
		int inboundMsgPerSec = 0;
		int aggregatorMsgInQue = 0;
		int aggregatorQueSize = 0;
		int aggregatorQueAge = 0;
		int storageWriterMsgPerSec = 0;
		int storageWriterBytesPerSec = 0;
		int storageWriterBatchesBeingWritten = 0;

/*
		int outboundMsgPerSec;
		int outboundAvgMsgSize;
		int outboundActiveQueries;
*/

		for (ServerStats serverStats : serverStatsList)
		{
			if (serverStats.getRoles().contains("Inbound"))
			{
				OverTime<AvgStat> iLoadStat = getAvgStatByName("System", "LoadAvg", serverStats);
				if (iLoadStat != null)
				{
					inboundLoad.add(iLoadStat.getSec5().getAverage());
				}

				OverTime<AvgStat> iAvgMsgSize = getAvgStatByName("Inbound", "AddMessage.Size", serverStats);
				if (iAvgMsgSize != null)
				{
					inboundAvgMsgSize.add(iAvgMsgSize.getSec5().getAverage());
				}

				OverTime<Long> iMsgPerSec = getCountStatByName("Inbound", "AddMessage.Count", serverStats);
				if (iMsgPerSec != null)
				{
					inboundMsgPerSec += iMsgPerSec.getSec5();
				}

				OverTime<AvgStat> aQueSize = getAvgStatByName("Aggregator", "TotalQueueSize.Avg", serverStats);
				if (aQueSize != null)
				{
					aggregatorQueSize += aQueSize.getSec5().getAverage();
				}

				OverTime<AvgStat> aMsgInQue = getAvgStatByName("Aggregator", "MessagesInQueue.Avg", serverStats);
				if (aMsgInQue != null)
				{
					aggregatorMsgInQue += aMsgInQue.getSec5().getAverage();
				}

				OverTime<AvgStat> aQueAge = getAvgStatByName("Aggregator", "OldestInQueue.Avg", serverStats);
				if (aQueAge != null)
				{
					if (aQueAge.getSec5().getAverage() > aggregatorQueAge)
					{
						aggregatorQueAge = Math.round(aQueAge.getSec5().getAverage());
					}
				}

				OverTime<Long> swMsgPerSec = getCountStatByName("Storage Writer", "MessagesWritten.Count", serverStats);
				if (swMsgPerSec != null)
				{
					storageWriterMsgPerSec += swMsgPerSec.getSec5();
				}
				OverTime<Long> swBytesPerSec = getCountStatByName("Storage Writer", "MessagesWritten.Size", serverStats);
				if (swBytesPerSec != null)
				{
					storageWriterBytesPerSec += swBytesPerSec.getSec5();
				}
				OverTime<AvgStat> swBatchesBeingWritten = getAvgStatByName("Storage Writer", "WritingBatches.Avg", serverStats);
				if (swBatchesBeingWritten != null)
				{
					storageWriterBatchesBeingWritten += swBatchesBeingWritten.getSec5().getAverage();
				}
			}
		}

		// Inbound
		clusterStats.getAggregateStats().setInboundLoad(inboundLoad.getAvg());
		clusterStats.getAggregateStats().setInboundAvgMsgSize(inboundAvgMsgSize.getAvg());
		if (inboundMsgPerSec > 0)
		{
			clusterStats.getAggregateStats().setInboundMsgPerSec(inboundMsgPerSec / 5);
		}

		// Aggregator
		clusterStats.getAggregateStats().setAggregatorMsgInQue(aggregatorMsgInQue);
		clusterStats.getAggregateStats().setAggregatorQueAge(aggregatorQueAge);
		clusterStats.getAggregateStats().setAggregatorQueSize(aggregatorQueSize);

		// Storage Writer
		clusterStats.getAggregateStats().setStorageWriterBatchesBeingWritten(storageWriterBatchesBeingWritten);
		if (storageWriterBytesPerSec > 0)
		{
			clusterStats.getAggregateStats().setStorageWriterBytesPerSec(storageWriterBytesPerSec / 5);
		}
		if (storageWriterMsgPerSec > 0)
		{
			clusterStats.getAggregateStats().setStorageWriterMsgPerSec(storageWriterMsgPerSec / 5);
		}

		return clusterStats;
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

	private OverTime<Long> getCountStatByName(String groupName, String statName, ServerStats serverStats)
	{
		for (GroupStats groupStats : serverStats.getGroupStatsList())
		{
			if (groupStats.getName().equalsIgnoreCase(groupName))
			{
				for (OverTime<Long> countStat : groupStats.getCountStats())
				{
					if (countStat.getName().equalsIgnoreCase(statName))
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
