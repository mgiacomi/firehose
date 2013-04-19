package com.gltech.scale.monitoring.model;

import com.dyuproject.protostuff.Tag;

import java.util.List;

public class ClusterStats
{
	@Tag(1)
	private AggregateStats aggregateStats = new AggregateStats();
	@Tag(2)
	private List<ServerStats> stats;

	public AggregateStats getAggregateStats()
	{
		return aggregateStats;
	}

	public List<ServerStats> getStats()
	{
		return stats;
	}

	public void setStats(List<ServerStats> stats)
	{
		this.stats = stats;
	}
}
