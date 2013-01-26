package com.gltech.scale.core.processor;

import com.gltech.scale.core.lifecycle.LifeCycle;
import com.gltech.scale.core.lifecycle.LifeCycleManager;
import com.gltech.scale.core.lifecycle.LifeCycleService;
import com.gltech.scale.core.util.Props;
import com.gltech.scale.core.util.UninterruptedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class AsynchProcessor<T> implements Processor<T>, LifeCycle
{
	private UninterruptedBlockingQueue<T> queue;

	private static final Logger logger = LoggerFactory.getLogger(AsynchProcessor.class);
	private Processor delegate;
	private LifeCycleService lifeCycleService;
	private int timeoutMillis;

	public AsynchProcessor(final Processor delegate, int threadCount, final String name, int maxQueueSize)
	{
		this.delegate = delegate;
		logger.info("starting " + delegate + " with " + threadCount + " threads ");
		Props.getProps().setVerbose(true);
		timeoutMillis = Props.getProps().get(name + "timeoutMillis", 100);
		Props.getProps().setVerbose(false);
		queue = new UninterruptedBlockingQueue<>(maxQueueSize);
		ExecutorService executorService = new ThreadPoolExecutor(threadCount, threadCount,
				60L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>(), new ThreadFactory()
		{
			public Thread newThread(Runnable r)
			{
				Thread thread = new Thread(r, delegate.toString() + "_" + name);
				thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
				{
					public void uncaughtException(Thread t, Throwable e)
					{
						logger.error("what happened here? " + t.getName(), e);
					}
				});
				logger.debug("starting " + name);
				return thread;
			}
		});
		List<Future> futures = new ArrayList<>();
		lifeCycleService = new LifeCycleService("AsynchProcessor", executorService, futures);
		for (int i = 0; i < threadCount; i++)
		{
			futures.add(executorService.submit(new QueueProcessor()));
		}
		LifeCycleManager.getInstance().add(lifeCycleService, Priority.INITIAL);
	}

	public void process(T event) throws QueueFullException
	{
		boolean offer = queue.offer(event, timeoutMillis, TimeUnit.MILLISECONDS);
		if (!offer)
		{
			throw new QueueFullException("queue is full, waited " + timeoutMillis + " millis");
		}
	}

	public void shutdown()
	{
		if (null != lifeCycleService)
		{
			lifeCycleService.shutdown();
		}
	}

	private class QueueProcessor implements Runnable
	{
		public void run()
		{
			while (!lifeCycleService.isShutdown())
			{
				process();
			}
			logger.info("shutting down " + this);
		}

		private void process()
		{
			T event = null;
			try
			{
				while (true)
				{
					event = queue.poll(timeoutMillis, TimeUnit.MILLISECONDS);
					if (null != event)
					{
						//noinspection unchecked
						delegate.process(event);
					}
					else
					{
						return;
					}
				}
			}
			catch (Error e)
			{
				logger.error("unexpected error processing " + event, e);
				throw e;
			}
			catch (Exception e)
			{
				logger.error("unexpected exception processing " + event, e);
			}
		}
	}

	public int getQueueSize()
	{
		return queue.size();
	}

	public String toString()
	{
		return "AsynchProcessor{" +
				"delegate=" + delegate +
				'}';
	}
}
