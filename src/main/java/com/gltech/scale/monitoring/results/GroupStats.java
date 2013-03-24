package com.gltech.scale.monitoring.results;

import java.util.ArrayList;
import java.util.List;

public class GroupStats
{
	private List<OverTime<AvgStat>> avgStats = new ArrayList<>();
	private List<OverTime<Long>> countStats = new ArrayList<>();

	public List<OverTime<AvgStat>> getAvgStats()
	{
		return avgStats;
	}

	public List<OverTime<Long>> getCountStats()
	{
		return countStats;
	}
}
