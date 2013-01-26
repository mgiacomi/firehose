package com.gltech.scale.core.monitor;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class TimerMap
{
	private ConcurrentMap<String, Timer> timerMap = new ConcurrentHashMap<>();

	public TimerMap()
	{
	}

	public TimerMap(String name, Timer timer)
	{
		timerMap.put(name, timer);
	}

	public Timer get(String name)
	{
		Timer timer = timerMap.get(name);
		if (null == timer)
		{
			timer = new Timer(name);
			Timer previous = timerMap.putIfAbsent(name, timer);
			if (null != previous)
			{
				return previous;
			}
		}
		return timer;
	}

	public Collection<Timer> getTimers()
	{
		return Collections.unmodifiableCollection(timerMap.values());
	}

	public void clear()
	{
		timerMap.clear();
	}
}
