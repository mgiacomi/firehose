package com.gltech.scale.monitoring.services;

import com.gltech.scale.core.stats.results.ResultsIO;
import com.gltech.scale.core.stats.results.ServerStats;
import com.google.inject.Inject;

import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

public class ClusterStatsServiceImpl implements ClusterStatsService
{
	private CopyOnWriteArraySet<ServerStats> serverStatsList = new CopyOnWriteArraySet<>();
	private ResultsIO resultsIO;

	@Inject
	public ClusterStatsServiceImpl(ResultsIO resultsIO)
	{
		this.resultsIO = resultsIO;
	}

	@Override
	public void updateGroupStats(ServerStats serverStats)
	{
		serverStatsList.add(serverStats);
	}

	@Override
	public String getJsonStatsAll()
	{
		return resultsIO.toJson(new ArrayList<>(serverStatsList));
	}

	@Override
	public String getJsonStatsByServer(String workerId)
	{
		for (ServerStats serverStats : serverStatsList)
		{
			if (serverStats.getWorkerId().equals(workerId))
			{
				return resultsIO.toJson(serverStats);
			}
		}

		return "{}";
	}
}
