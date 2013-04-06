package com.gltech.scale.monitoring.services;

import com.gltech.scale.core.stats.results.ResultsIO;
import com.gltech.scale.core.stats.results.ServerStats;
import com.google.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ClusterStatsServiceImpl implements ClusterStatsService
{
	private ConcurrentMap<String, ServerStats> serverStatsMap = new ConcurrentHashMap<>();
	private ResultsIO resultsIO;

	@Inject
	public ClusterStatsServiceImpl(ResultsIO resultsIO)
	{
		this.resultsIO = resultsIO;
	}

	@Override
	public void updateGroupStats(ServerStats serverStats)
	{
		serverStatsMap.put(serverStats.getWorkerId(), serverStats);
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

		return resultsIO.toJson(sortedStatsList);
	}

	@Override
	public String getJsonStatsByServer(String workerId)
	{
		return resultsIO.toJson(serverStatsMap.get(workerId));
	}
}
