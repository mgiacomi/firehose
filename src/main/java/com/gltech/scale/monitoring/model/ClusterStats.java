package com.gltech.scale.monitoring.model;

import com.dyuproject.protostuff.Tag;

import java.util.*;

public class ClusterStats
{
	@Tag(1)
	Map<String, Map<String, AggregateOverTime>> aggregateStats = new HashMap<>();
	@Tag(2)
	private List<ServerStats> stats = new ArrayList<>();
	@Tag(2)
	private Map<String, List<BatchStatus>> activeBatches = new TreeMap<>();
	@Tag(3)
	private List<PeriodStatus> periodStatuses = new ArrayList<>();

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

	public Map<String, List<BatchStatus>> getActiveBatches()
	{
		return activeBatches;
	}

	public void setActiveBatches(Map<String, List<BatchStatus>> activeBatches)
	{
		this.activeBatches = activeBatches;
	}

	public List<PeriodStatus> getPeriodStatuses()
	{
		return periodStatuses;
	}

	public void setPeriodStatuses(List<PeriodStatus> periodStatuses)
	{
		this.periodStatuses = periodStatuses;
	}
}
