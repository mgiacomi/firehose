package com.gltech.scale.core.stats;

import com.gltech.scale.core.cluster.TimePeriodUtils;
import org.joda.time.DateTime;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class CounterStatOverTime implements StatOverTime
{
	private ConcurrentMap<DateTime, AtomicLong> countsBy5SecPeriods = new ConcurrentHashMap<>();
	private String statName;

	// Only allow classes in this package to create a stat.
	protected CounterStatOverTime(String statName) {
		this.statName = statName;
	}

	public void increment()
	{
		add(1, DateTime.now());
	}

	public void add(long count)
	{
		add(count, DateTime.now());
	}

	void add(DateTime dateTime)
	{
		add(1, dateTime);
	}

	void add(long count, DateTime dateTime)
	{
		DateTime period = TimePeriodUtils.nearestPeriodCeiling(dateTime, 5);
		AtomicLong atomicTotal = countsBy5SecPeriods.get(period);

		if (atomicTotal == null)
		{
			AtomicLong newAtomicTotal = new AtomicLong(0);
			atomicTotal = countsBy5SecPeriods.putIfAbsent(period, newAtomicTotal);
			if (atomicTotal == null)
			{
				atomicTotal = newAtomicTotal;
			}
		}

		atomicTotal.addAndGet(count);
	}

	public String getStatName()
	{
		return statName;
	}

	public long getCountOverSeconds(int seconds)
	{
		return getTotalCount(seconds / 5 + 1);
	}

	public long getCountOverMinutes(int minutes)
	{
		return getTotalCount(minutes * 12);
	}

	public long getCountOverHours(int hour)
	{
		return getTotalCount(60 * 12 * hour);
	}

	long getTotalCount(int loops)
	{
		if(loops > 1440) {
			throw new IllegalArgumentException("You can only query 2 hours back in time.");
		}

		DateTime period = TimePeriodUtils.nearestPeriodCeiling(DateTime.now(), 5);
		long counter = 0;

		for (int i = 0; i < loops; i++)
		{
			AtomicLong atomicLong = countsBy5SecPeriods.get(period);

			if (atomicLong != null)
			{
				counter += atomicLong.get();
			}

			period = period.minusSeconds(5);
		}

		return counter;
	}

	@Override
	public void cleanOldThanTwoHours()
	{
		for(DateTime period : countsBy5SecPeriods.keySet())
		{
			DateTime twoHoursAgo = DateTime.now().minusHours(2);
			if(period.isAfter(twoHoursAgo))
			{
				countsBy5SecPeriods.remove(twoHoursAgo);
			}
		}
	}

	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		CounterStatOverTime that = (CounterStatOverTime) o;

		if (statName != null ? !statName.equals(that.statName) : that.statName != null) return false;

		return true;
	}

	public int hashCode()
	{
		return statName != null ? statName.hashCode() : 0;
	}
}
