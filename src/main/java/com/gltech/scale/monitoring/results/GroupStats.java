package com.gltech.scale.monitoring.results;

import com.dyuproject.protostuff.Tag;

import java.util.ArrayList;
import java.util.List;

public class GroupStats
{
	@Tag(1)
	private String groupName;
	@Tag(2)
	private List<OverTime<AvgStat>> avgStats = new ArrayList<>();
	@Tag(3)
	private List<OverTime<Long>> countStats = new ArrayList<>();

	public GroupStats(String groupName)
	{
		this.groupName = groupName;
	}

	public String getName()
	{
		return groupName;
	}

	public List<OverTime<AvgStat>> getAvgStats()
	{
		return avgStats;
	}

	public List<OverTime<Long>> getCountStats()
	{
		return countStats;
	}
}
