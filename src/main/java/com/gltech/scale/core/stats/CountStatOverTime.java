package com.gltech.scale.core.stats;

import com.gltech.scale.core.cluster.TimePeriodUtils;
import org.joda.time.DateTime;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class CountStatOverTime implements StatOverTime
{
	private ConcurrentMap<DateTime, AtomicLong> countsBySecs = new ConcurrentHashMap<>();
	private final ThreadLocal<Long> startTime = new ThreadLocal<>();
	private String statName;
	private String unitOfMeasure;

	// Only allow classes in this package to create a stat.
	protected CountStatOverTime(String statName, String unitOfMeasure)
	{
		this.statName = statName;
		this.unitOfMeasure = unitOfMeasure;
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
		DateTime period = TimePeriodUtils.nearestPeriodCeiling(dateTime, 1);
		AtomicLong atomicTotal = countsBySecs.get(period);

		if (atomicTotal == null)
		{
			AtomicLong newAtomicTotal = new AtomicLong(0);
			atomicTotal = countsBySecs.putIfAbsent(period, newAtomicTotal);
			if (atomicTotal == null)
			{
				atomicTotal = newAtomicTotal;
			}
		}

		atomicTotal.addAndGet(count);
	}

	@Override
	public String getName()
	{
		return statName;
	}

	protected void setStatName(String statName)
	{
		this.statName = statName;
	}

	@Override
	public String getUnitOfMeasure()
	{
		return unitOfMeasure;
	}

	protected void setUnitOfMeasure(String unitOfMeasure)
	{
		this.unitOfMeasure = unitOfMeasure;
	}

	public long getCountOverSeconds(int seconds)
	{
		return getTotalCount(seconds);
	}

	public long getCountOverMinutes(int minutes)
	{
		return getTotalCount(minutes * 60);
	}

	public long getCountOverHours(int hour)
	{
		return getTotalCount(60 * 60 * hour);
	}

	long getTotalCount(int loops)
	{
		if (loops > 7200)
		{
			throw new IllegalArgumentException("You can only query 2 hours back in time.");
		}

		DateTime period = TimePeriodUtils.nearestPeriodCeiling(DateTime.now(), 1);
		long counter = 0;

		for (int i = 0; i < loops; i++)
		{
			period = period.minusSeconds(1);

			AtomicLong atomicLong = countsBySecs.get(period);

			if (atomicLong != null)
			{
				counter += atomicLong.get();
			}
		}

		return counter;
	}

	@Override
	public void cleanOldThanTwoHours()
	{
		for (DateTime period : countsBySecs.keySet())
		{
			DateTime twoHoursAgo = DateTime.now().minusHours(2).minusMinutes(1);
			if (period.isAfter(twoHoursAgo))
			{
				countsBySecs.remove(twoHoursAgo);
			}
		}
	}

	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		CountStatOverTime that = (CountStatOverTime) o;

		if (statName != null ? !statName.equals(that.statName) : that.statName != null) return false;

		return true;
	}

	public int hashCode()
	{
		return statName != null ? statName.hashCode() : 0;
	}
}
