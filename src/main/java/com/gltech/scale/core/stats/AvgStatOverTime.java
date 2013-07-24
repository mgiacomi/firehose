package com.gltech.scale.core.stats;

import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.core.stats.results.AvgStat;
import org.joda.time.DateTime;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class AvgStatOverTime implements StatOverTime
{
	private ConcurrentMap<DateTime, AtomicLong> totalsBySecs = new ConcurrentHashMap<>();
	private final ThreadLocal<Long> startTime = new ThreadLocal<>();
	private final CountStatOverTime countStatOverTime;
	private String statName;
	private String unitOfMeasure;

	// Only allow classes in this package to create a stat.
	protected AvgStatOverTime(String statName, String unitOfMeasure)
	{
		this.statName = statName;
		this.unitOfMeasure = unitOfMeasure;
		countStatOverTime = new CountStatOverTime(null, null);
	}

	// Only allow classes in this package to create a stat.
	public void activateCountStat(String countStatName, String unitOfMeasure)
	{
		countStatOverTime.setStatName(countStatName);
		countStatOverTime.setUnitOfMeasure(unitOfMeasure);
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
		DateTime period = TimePeriodUtils.nearestPeriodCeiling(dateTime, 1);
		AtomicLong atomicTotal = totalsBySecs.get(period);

		if (atomicTotal == null)
		{
			AtomicLong newAtomicTotal = new AtomicLong(0);
			atomicTotal = totalsBySecs.putIfAbsent(period, newAtomicTotal);
			if (atomicTotal == null)
			{
				atomicTotal = newAtomicTotal;
			}
		}

		atomicTotal.addAndGet(total);
		countStatOverTime.add(dateTime);
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

	public CountStatOverTime getCountStatOverTime()
	{
		return countStatOverTime;
	}

	public AvgStat getAvgOverSeconds(int seconds)
	{
		return getAverage(seconds);
	}

	public AvgStat getAvgOverMinutes(int minutes)
	{
		return getAverage(minutes * 60);
	}

	public AvgStat getAvgOverHours(int hour)
	{
		return getAverage(60 * 60 * hour);
	}

	private AvgStat getAverage(int loops)
	{
		if (loops > 7200)
		{
			throw new IllegalArgumentException("You can only query 2 hours back in time.");
		}

		DateTime period = TimePeriodUtils.nearestPeriodCeiling(DateTime.now(), 1);
		long total = 0;

		for (int i = 0; i < loops; i++)
		{
			period = period.minusSeconds(1);

			AtomicLong atomicLong = totalsBySecs.get(period);

			if (atomicLong != null)
			{
				total += atomicLong.get();
			}
		}

		long counter = countStatOverTime.getTotalCount(loops);
		return new AvgStat(total, counter);
	}

	@Override
	public void cleanOldThanTwoHours()
	{
		for (DateTime period : totalsBySecs.keySet())
		{
			DateTime twoHoursAgo = DateTime.now().minusHours(2).minusMinutes(1);
			if (period.isAfter(twoHoursAgo))
			{
				totalsBySecs.remove(twoHoursAgo);
			}
		}

		countStatOverTime.cleanOldThanTwoHours();
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
