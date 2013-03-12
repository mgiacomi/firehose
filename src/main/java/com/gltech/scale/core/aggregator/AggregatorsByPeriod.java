package com.gltech.scale.core.aggregator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import org.joda.time.DateTime;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class AggregatorsByPeriod
{
	private final DateTime period;
	private final List<PrimaryBackupSet> primaryBackupSets;
	private final List<ServiceMetaData> ropeManagers = new ArrayList<>();
	private final AtomicInteger setIndex = new AtomicInteger(0);
//	private final AtomicInteger singleIndex = new AtomicInteger(0);

	@JsonCreator
	public AggregatorsByPeriod(@JsonProperty("period") DateTime period, @JsonProperty("primaryBackupSets") List<PrimaryBackupSet> primaryBackupSets)
	{
		this.period = period;
		this.primaryBackupSets = primaryBackupSets;

		for (PrimaryBackupSet primaryBackupSet : primaryBackupSets)
		{
			ropeManagers.add(primaryBackupSet.getPrimary());
			if (primaryBackupSet.getBackup() != null)
			{
				ropeManagers.add(primaryBackupSet.getBackup());
			}
		}
	}

	public DateTime getPeriod()
	{
		return period;
	}

	public List<PrimaryBackupSet> getPrimaryBackupSets()
	{
		return primaryBackupSets;
	}

	public ServiceMetaData next()
	{
		if (ropeManagers.size() == 0)
		{
			return null;
		}

		int singleIndex = Math.abs(setIndex.getAndIncrement());
		return ropeManagers.get(singleIndex % ropeManagers.size());
	}

	public PrimaryBackupSet nextPrimaryBackupSet()
	{
		if (primaryBackupSets.size() == 0)
		{
			return null;
		}

		int thisIndex = Math.abs(setIndex.getAndIncrement());
		return primaryBackupSets.get(thisIndex % primaryBackupSets.size());
	}
}
