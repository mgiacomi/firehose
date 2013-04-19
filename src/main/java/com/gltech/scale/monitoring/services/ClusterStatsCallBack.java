package com.gltech.scale.monitoring.services;

import com.gltech.scale.monitoring.model.ClusterStats;

public interface ClusterStatsCallBack
{
	void clusterStatsUpdate(ClusterStats clusterStats);
}
