package com.gltech.scale.core.processor;

import com.gltech.scale.core.util.ThreadSleep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class CountingProcessor<T> implements Processor<T>
{
	private static final Logger logger = LoggerFactory.getLogger(CountingProcessor.class);

	private final AtomicInteger count = new AtomicInteger();
	private T t;

	public void process(T event)
	{
		this.t = event;
		count.incrementAndGet();
	}

	public int getCount()
	{
		return count.get();
	}

	public boolean waitForCount(int expectedCount, int timeoutMillis)
	{
		long start = System.currentTimeMillis();
		while (count.get() < expectedCount)
		{
			if (System.currentTimeMillis() - timeoutMillis > start)
			{
				logger.info("waited too long " + (System.currentTimeMillis() - start) + " for " + count.get());
				return false;
			}
			ThreadSleep.sleep(10);
		}
		logger.info("waited " + (System.currentTimeMillis() - start) + " for " + count.get());
		return true;
	}

	public T getProcessed()
	{
		return t;
	}
}
