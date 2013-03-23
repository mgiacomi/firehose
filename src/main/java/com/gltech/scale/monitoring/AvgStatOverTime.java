package com.gltech.scale.monitoring;

import com.gltech.scale.core.cluster.TimePeriodUtils;
import org.joda.time.DateTime;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class AvgStatOverTime implements StatOverTime
{
	private ConcurrentMap<DateTime, AtomicLong> totalsBy5SecPeriods = new ConcurrentHashMap<>();
	private CounterStatOverTime counterStatOverTime = new CounterStatOverTime();

	// Only allow classes in this package to create a stat.
	protected AvgStatOverTime() {}

	public void add(long total)
	{
		add(total, DateTime.now());
	}

	void add(long total, DateTime dateTime)
	{
		DateTime period = TimePeriodUtils.nearestPeriodCeiling(dateTime, 5);
		AtomicLong atomicTotal = totalsBy5SecPeriods.get(period);

		if (atomicTotal == null)
		{
			AtomicLong newAtomicTotal = new AtomicLong(0);
			atomicTotal = totalsBy5SecPeriods.putIfAbsent(period, newAtomicTotal);
			if (atomicTotal == null)
			{
				atomicTotal = newAtomicTotal;
			}
		}

		atomicTotal.addAndGet(total);
		counterStatOverTime.increment(dateTime);
	}

	public AvgCountStat getAvgOverSeconds(int seconds)
	{
		return getAverage(seconds / 5 + 1);
	}

	public AvgCountStat getAvgOverMinutes(int minutes)
	{
		return getAverage(minutes * 12);
	}

	public AvgCountStat getAvgOverHours(int hour)
	{
		return getAverage(60 * 12 * hour);
	}

	private AvgCountStat getAverage(int loops)
	{
		if(loops > 1440) {
			throw new IllegalArgumentException("You can only query 2 hours back in time.");
		}

		DateTime period = TimePeriodUtils.nearestPeriodCeiling(DateTime.now(), 5);
		long total = 0;

		for (int i = 0; i < loops; i++)
		{
			AtomicLong atomicLong = totalsBy5SecPeriods.get(period);

			if (atomicLong != null)
			{
				total += atomicLong.get();
			}

			period = period.minusSeconds(5);
		}

		long counter = counterStatOverTime.getTotalCount(loops);
		return new AvgCountStat(total, counter);
	}

	@Override
	public void cleanOldThanTwoHours()
	{
		for(DateTime period : totalsBy5SecPeriods.keySet())
		{
			DateTime twoHoursAgo = DateTime.now().minusHours(2);
			if(period.isAfter(twoHoursAgo))
			{
				totalsBy5SecPeriods.remove(twoHoursAgo);
			}
		}

		counterStatOverTime.cleanOldThanTwoHours();
	}
}
