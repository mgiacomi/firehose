package com.gltech.scale.core.monitor;

import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class Timer
{
	private AtomicLong time = new AtomicLong();
	private AtomicLong count = new AtomicLong();
	private String name = "";
	private long startNanos;

	public Timer()
	{
	}

	public Timer(String name)
	{
		this.name = name;
	}

	public void start()
	{
		startNanos = System.nanoTime();
	}

	public void stop()
	{
		add(System.nanoTime() - startNanos);
		startNanos = 0;
	}

	public String getName()
	{
		return name;
	}

	public void add(long nanos)
	{
		add(nanos, 1);
	}

	public void addMillis(long millis)
	{
		add(millis * 1000 * 1000, 1);
	}

	public void add(long nanos, long increment)
	{
		this.time.addAndGet(nanos);
		count.getAndAdd(increment);
	}

	public long getCount()
	{
		return count.get();
	}

	public long getNanoTime()
	{
		return time.get();
	}

	public long getMillisTime()
	{
		return time.get() / 1000 / 1000;
	}

	public double getAverage()
	{
		double nanosPerCount = (double) getNanoTime() / (double) getCount();
		return nanosPerCount / 1000 / 1000;
	}

	public String toString()
	{
		DecimalFormat decimalFormat = new DecimalFormat("###,###,##0");
		DecimalFormat averageFormat = new DecimalFormat("###,##0.00");
		long number = getCount();
		if (number == 0)
		{
			return "count = 0";
		}
		long millis = getMillisTime();
		double average = (double) millis / (double) number;
		return "name=" + name +
				", count=" + decimalFormat.format(number) +
				", time(ms)=" + decimalFormat.format(millis) +
				", average(ms)=" + averageFormat.format(average);
	}
}
