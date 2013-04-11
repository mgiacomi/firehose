package com.gltech.scale.monitoring.services;

import com.gltech.scale.monitoring.model.ServerStats;

import java.util.List;

public interface ClusterStatsService
{
	void updateGroupStats(List<ServerStats> serverStatsList);

	String getJsonStatsAll();

	void registerCallback(ClusterStatsCallBack clusterStatsCallBack);

	void unRegisterCallback(ClusterStatsCallBack clusterStatsCallBack);
}
