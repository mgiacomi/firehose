package com.gltech.scale.monitoring.services;

import com.gltech.scale.core.stats.results.ServerStats;

public interface ClusterStatsService
{
	void updateGroupStats(ServerStats serverStats);

	String getJsonStatsAll();

	String getJsonStatsByServer(String workerId);

	void registerCallback(ClusterStatsCallBack clusterStatsCallBack);

	void unRegisterCallback(ClusterStatsCallBack clusterStatsCallBack);
}
