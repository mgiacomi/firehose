package com.gltech.scale.core.stats;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class StatsThreadPoolExecutor extends ThreadPoolExecutor
{
	private final ThreadLocal<Long> startTime = new ThreadLocal<>();
	private final AvgStatOverTime avgStatOverTime;

	public StatsThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
								   BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, AvgStatOverTime avgStatOverTime)
	{
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
		this.avgStatOverTime = avgStatOverTime;
	}

	protected void beforeExecute(Thread t, Runnable r)
	{
		super.beforeExecute(t, r);
		startTime.set(System.nanoTime()  / 1000 / 1000);
	}

	protected void afterExecute(Runnable r, Throwable t)
	{
		try
		{
			avgStatOverTime.add((System.nanoTime()  / 1000 / 1000) - startTime.get());
		}
		finally
		{
			super.afterExecute(r, t);
		}
	}
}
