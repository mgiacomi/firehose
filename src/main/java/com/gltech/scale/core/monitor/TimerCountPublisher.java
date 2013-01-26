package com.gltech.scale.core.monitor;

import java.util.Collection;

/**
 *
 */
public class TimerCountPublisher implements PublishCallback
{
	private TimerMap timerMap;
	long lastCount = 0;

	public TimerCountPublisher(TimerMap timerMap)
	{
		this.timerMap = timerMap;
	}

	public TimerCountPublisher(String name, Timer timer)
	{
		this.timerMap = new TimerMap(name, timer);
	}

	public String getValue()
	{
		long count = 0;
		Collection<Timer> timers = timerMap.getTimers();
		for (Timer timer : timers)
		{
			count += timer.getCount();
		}
		long returnCount = count - lastCount;
		lastCount = count;
		return String.valueOf(returnCount);
	}
}
