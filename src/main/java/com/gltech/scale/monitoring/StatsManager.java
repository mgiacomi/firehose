package com.gltech.scale.monitoring;

import com.gltech.scale.lifecycle.LifeCycle;
import com.gltech.scale.monitoring.results.GroupStats;

import java.util.ArrayList;

public interface StatsManager extends LifeCycle
{
	void start();

	AvgStatOverTime createAvgStat(String groupName, String statName);

	AvgStatOverTime createAvgStat(String groupName, String statName, StatCallBack statCallBack);

	AvgStatOverTime createAvgAndCountStat(String groupName, String avgStatName, String countStatName);

	AvgStatOverTime createAvgAndCountStat(String groupName, String avgStatName, String countStatName, StatCallBack statCallBack);

	CounterStatOverTime createCounterStat(String groupName, String statName);

	CounterStatOverTime createCounterStat(String groupName, String statName, StatCallBack statCallBack);

	byte[] toBytes();

	String toJson();

	ArrayList<GroupStats> getGroupStats();
}
