package com.gltech.scale.monitoring;

import com.gltech.scale.lifecycle.LifeCycle;
import com.gltech.scale.monitoring.results.GroupStats;

import java.util.ArrayList;

public interface StatsManager extends Runnable, LifeCycle
{
	AvgStatOverTime createAvgStat(String groupName, String statName);

	CounterStatOverTime createCounterStat(String groupName, String statName);

	byte[] toBytes();

	String toJson();

	ArrayList<GroupStats> getGroupStats();
}
