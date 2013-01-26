package com.gltech.scale.core.lifecycle;

import com.gltech.scale.core.util.ThreadSleep;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static junit.framework.Assert.assertTrue;

/**
 *
 */
public class LifeCycleIntegrationTest
{

	@Test
	public void testService() throws Exception
	{
		AtomicLong initial = new AtomicLong();
		addService(initial, LifeCycle.Priority.INITIAL);
		AtomicLong middle = new AtomicLong();
		addService(middle, LifeCycle.Priority.SECOND);
		AtomicLong finalL = new AtomicLong();
		addService(finalL, LifeCycle.Priority.FINAL);

		LifeCycleManager.getInstance().shutdown();

		assertTrue("initial=" + initial.get() + " middle=" + middle.get(), initial.get() <= middle.get());
		assertTrue("middle=" + middle.get() + " finalL=" + middle.get(), middle.get() <= finalL.get());
	}

	private void addService(final AtomicLong initial, LifeCycle.Priority priority)
	{
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		final LifeCycleService initialService = new LifeCycleService("initial", executorService);
		executorService.submit(new Runnable()
		{
			public void run()
			{
				while (!initialService.isShutdown())
				{
					ThreadSleep.sleep(10);
				}
				initial.set(System.currentTimeMillis());
			}
		});

		LifeCycleManager.getInstance().add(initialService, priority);
	}

}
