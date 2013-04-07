package com.gltech.scale.monitoring.services;

import com.gltech.scale.core.stats.results.ServerStats;

public interface ClusterStatsCallBack
{
	void serverStatsUpdate(ServerStats serverStats);
}
