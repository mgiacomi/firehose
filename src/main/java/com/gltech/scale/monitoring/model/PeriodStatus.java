package com.gltech.scale.monitoring.model;

import com.dyuproject.protostuff.Tag;
import com.gltech.scale.core.aggregator.PrimaryBackupSet;

import java.util.ArrayList;
import java.util.List;

public class PeriodStatus
{
	@Tag(1)
	private String period;
	@Tag(2)
	private List<PrimaryBackupSet> primaryBackupSets = new ArrayList<>();

	public String getPeriod()
	{
		return period;
	}

	public void setPeriod(String period)
	{
		this.period = period;
	}

	public List<PrimaryBackupSet> getPrimaryBackupSets()
	{
		return primaryBackupSets;
	}

	public void setPrimaryBackupSets(List<PrimaryBackupSet> primaryBackupSets)
	{
		this.primaryBackupSets = primaryBackupSets;
	}
}
