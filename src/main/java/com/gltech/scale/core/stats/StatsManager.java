package com.gltech.scale.core.stats;

import com.gltech.scale.core.stats.results.GroupStats;
import com.gltech.scale.lifecycle.LifeCycle;

import java.util.ArrayList;

public interface StatsManager extends LifeCycle
{
	void start();

	AvgStatOverTime createAvgStat(String groupName, String statName, String unitOfMeasure);

	AvgStatOverTime createAvgStat(String groupName, String statName, String unitOfMeasure, StatCallBack statCallBack);

	CounterStatOverTime createCounterStat(String groupName, String statName, String unitOfMeasure);

	CounterStatOverTime createCounterStat(String groupName, String statName, String unitOfMeasure, StatCallBack statCallBack);

	byte[] toBytes();

	String toJson();

	ArrayList<GroupStats> getGroupStats();
}
