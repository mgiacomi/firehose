package com.gltech.scale.core.stats.results;

import com.dyuproject.protostuff.Tag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroupStats
{
	@Tag(1)
	private String groupName;
	@Tag(2)
	private Map<String, OverTime<AvgStat>> avgStats = new HashMap<>();
	@Tag(3)
	private Map<String, OverTime<Long>> countStats = new HashMap<>();

	public GroupStats(String groupName)
	{
		this.groupName = groupName;
	}

	public String getName()
	{
		return groupName;
	}

	public Map<String, OverTime<AvgStat>> getAvgStats()
	{
		return avgStats;
	}

	public Map<String, OverTime<Long>> getCountStats()
	{
		return countStats;
	}

	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		GroupStats that = (GroupStats) o;

		if (groupName != null ? !groupName.equals(that.groupName) : that.groupName != null) return false;

		return true;
	}

	public int hashCode()
	{
		return groupName != null ? groupName.hashCode() : 0;
	}
}
