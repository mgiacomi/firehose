package com.gltech.scale.core.stats;

import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.core.stats.results.AvgStat;
import org.joda.time.DateTime;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class AvgStatOverTime implements StatOverTime
{
	private ConcurrentMap<DateTime, AtomicLong> totalsBy5SecPeriods = new ConcurrentHashMap<>();
	private final ThreadLocal<Long> startTime = new ThreadLocal<>();
	private final CounterStatOverTime counterStatOverTime;
	private String statName;
	private String unitOfMeasure;

	// Only allow classes in this package to create a stat.
	protected AvgStatOverTime(String statName, String unitOfMeasure)
	{
		this.statName = statName;
		this.unitOfMeasure = unitOfMeasure;
		counterStatOverTime = new CounterStatOverTime(null, null);
	}

	// Only allow classes in this package to create a stat.
	public void activateCountStat(String countStatName, String unitOfMeasure)
	{
		counterStatOverTime.setStatName(countStatName);
		counterStatOverTime.setUnitOfMeasure(unitOfMeasure);
	}


	@Override
	public void startTimer()
	{
		startTime.set(System.nanoTime() / 1000 / 1000);
	}

	@Override
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
		counterStatOverTime.add(dateTime);
	}

	@Override
	public String getName()
	{
		return statName;
	}

	@Override
	public String getUnitOfMeasure()
	{
		return unitOfMeasure;
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
