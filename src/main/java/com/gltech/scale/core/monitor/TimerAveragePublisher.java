package com.gltech.scale.core.monitor;

import java.util.Collection;

/**
 *
 */
public class TimerAveragePublisher implements PublishCallback
{
	private TimerMap timerMap;
	long totalCount = 0;
	long totalTime = 0;
	String previousAverage = "0.0";

	public TimerAveragePublisher(TimerMap timerMap)
	{
		this.timerMap = timerMap;
	}

	public TimerAveragePublisher(String name, Timer timer)
	{
		this.timerMap = new TimerMap(name, timer);
	}

	public String getValue()
	{
		long count = -totalCount;
		long nanoTime = -totalTime;
		totalCount = 0;
		totalTime = 0;
		Collection<Timer> timers = timerMap.getTimers();
		for (Timer timer : timers)
		{
			nanoTime += timer.getNanoTime();
			count += timer.getCount();
			totalCount += timer.getCount();
			totalTime += timer.getNanoTime();
		}
		if (count > 0)
		{
			double average = (double) nanoTime / (double) count / 1000 / 1000;
			previousAverage = String.valueOf(average);
		}
		return previousAverage;
	}
}
