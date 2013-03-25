package com.gltech.scale.monitoring;

import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.monitoring.results.AvgStat;
import org.joda.time.DateTime;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class AvgStatOverTime implements StatOverTime
{
	private ConcurrentMap<DateTime, AtomicLong> totalsBy5SecPeriods = new ConcurrentHashMap<>();
	private final ThreadLocal<Long> startTime = new ThreadLocal<>();
	private CounterStatOverTime counterStatOverTime;
	private String statName;

	// Only allow classes in this package to create a stat.
	protected AvgStatOverTime(String statName)
	{
		this.statName = statName;
		counterStatOverTime = new CounterStatOverTime(null);
	}

	// Only allow classes in this package to create a stat.
	protected AvgStatOverTime(String statName, String countStatName)
	{
		this.statName = statName;
		counterStatOverTime = new CounterStatOverTime(countStatName);
	}

	public void startTimer()
	{
		startTime.set(System.nanoTime()  / 1000 / 1000);
	}

	public void stopTimer()
	{
		add((System.nanoTime() / 1000 / 1000) - startTime.get());
	}

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

	@Override
	public String getStatName()
	{
		return statName;
	}

	public CounterStatOverTime getCounterStatOverTime()
	{
		return counterStatOverTime;
	}

	public AvgStat getAvgOverSeconds(int seconds)
	{
		return getAverage(seconds / 5 + 1);
	}

	public AvgStat getAvgOverMinutes(int minutes)
	{
		return getAverage(minutes * 12);
	}

	public AvgStat getAvgOverHours(int hour)
	{
		return getAverage(60 * 12 * hour);
	}

	private AvgStat getAverage(int loops)
	{
		if (loops > 1440)
		{
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
		return new AvgStat(total, counter);
	}

	@Override
	public void cleanOldThanTwoHours()
	{
		for (DateTime period : totalsBy5SecPeriods.keySet())
		{
			DateTime twoHoursAgo = DateTime.now().minusHours(2);
			if (period.isAfter(twoHoursAgo))
			{
				totalsBy5SecPeriods.remove(twoHoursAgo);
			}
		}

		counterStatOverTime.cleanOldThanTwoHours();
	}

	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		AvgStatOverTime that = (AvgStatOverTime) o;

		if (statName != null ? !statName.equals(that.statName) : that.statName != null) return false;

		return true;
	}

	public int hashCode()
	{
		return statName != null ? statName.hashCode() : 0;
	}
}
