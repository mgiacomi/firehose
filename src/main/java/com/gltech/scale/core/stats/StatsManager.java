package com.gltech.scale.core.stats;

import com.gltech.scale.monitoring.model.ServerStats;
import com.gltech.scale.lifecycle.LifeCycle;

public interface StatsManager extends LifeCycle
{
	void start();

	AvgStatOverTime createAvgStat(String groupName, String statName, String unitOfMeasure);

	AvgStatOverTime createAvgStat(String groupName, String statName, String unitOfMeasure, StatCallBack statCallBack);

	CounterStatOverTime createCounterStat(String groupName, String statName, String unitOfMeasure);

	CounterStatOverTime createCounterStat(String groupName, String statName, String unitOfMeasure, StatCallBack statCallBack);

	ServerStats getServerStats();
}
