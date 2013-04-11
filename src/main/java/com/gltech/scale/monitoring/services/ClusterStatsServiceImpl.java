package com.gltech.scale.monitoring.services;

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

		for (ClusterStatsCallBack clusterStatsCallBack : callBacks)
		{
			clusterStatsCallBack.serverStatsUpdate(serverStatsList);
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

		return resultsIO.toJson(sortedStatsList);
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
}
