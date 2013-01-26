package com.gltech.scale.core.monitor;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;

public class TimerThreadPoolExecutor extends ThreadPoolExecutor
{
	private final ThreadLocal<Long> startTime = new ThreadLocal<>();
	private final Timer timer;

	public TimerThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
								   BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, Timer timer)
	{
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
		this.timer = timer;
	}

	protected void beforeExecute(Thread t, Runnable r)
	{
		super.beforeExecute(t, r);
		startTime.set(System.nanoTime());
	}

	protected void afterExecute(Runnable r, Throwable t)
	{
		try
		{
			timer.add(System.nanoTime() - startTime.get());
		}
		finally
		{
			super.afterExecute(r, t);
		}
	}
}
