package com.gltech.scale.monitoring.services;

import com.gltech.scale.monitoring.model.ServerStats;

import java.util.List;

public interface ClusterStatsCallBack
{
	void serverStatsUpdate(List<ServerStats> serverStatsList);
}
