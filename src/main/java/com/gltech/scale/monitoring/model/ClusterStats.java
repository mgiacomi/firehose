package com.gltech.scale.monitoring.model;

import com.dyuproject.protostuff.Tag;

import java.util.*;

public class ClusterStats
{
	@Tag(1)
	Map<String, Map<String, AggregateOverTime>> aggregateStats = new HashMap<>();
	@Tag(2)
	private List<ServerStats> stats;

	public Map<String, Map<String, AggregateOverTime>> getAggregateStats()
	{
		return aggregateStats;
	}

	public void setAggregateStats(Map<String, Map<String, AggregateOverTime>> aggregateStats)
	{
		this.aggregateStats = aggregateStats;
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
