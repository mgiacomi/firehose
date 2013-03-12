package com.gltech.scale.pipeline;

import org.apache.commons.lang.StringUtils;

/**
 * PipelineBuilder simplifies chaining together multiple Processors.
 */
@SuppressWarnings({"unchecked"})
public class PipelineBuilder
{
	private Processor delegateProcessor;
	private String name;
	private int threadCount = -1;
	private boolean asynchronous;
	private boolean statistics;
	private String groupName;
	private int maxQueueSize;

	/**
	 * Create a PipelineBuilder with required elements.
	 *
	 * @param delegateProcessor required - this does the actual work of your system.
	 * @param name              required - a name that is unique to your application.
	 */
	public PipelineBuilder(Processor delegateProcessor, String name)
	{
		this.delegateProcessor = delegateProcessor;
		this.name = name;
	}

	/**
	 * Perform work asynchronously.
	 *
	 * @param threadCount
	 * @return
	 */
	public PipelineBuilder asynchronous(int threadCount, int maxQueueSize)
	{
		this.maxQueueSize = maxQueueSize;
		this.asynchronous = true;
		this.threadCount = threadCount;
		return this;
	}

	/**
	 * Collects the count and average time statistics.  The results are available from the
	 * Pipeline and are also published to Ganglia using MonitoringPublisher.
	 *
	 * @return
	 */
	public PipelineBuilder statistics(String groupName)
	{
		this.groupName = groupName;
		this.statistics = true;
		return this;
	}

	/**
	 * Create the verfified Processor.
	 *
	 * @param <T> The type expected
	 * @return
	 * @throws Exception
	 */
	public <T> Pipeline<T> build() throws Exception
	{
		verifyBuild();
		PipelineImpl pipeline = new PipelineImpl();
		pipeline.setName(name);
		Processor nextInChain = delegateProcessor;
		/**
		 * The pipeline is built from the bottom up, the top of this list is the closest
		 * to the delegateProcessor, and the bottom is the first in the chain.
		 */
		if (statistics)
		{
			StatisticsCompleteProcessor statisticsCompleteProcessor = new StatisticsCompleteProcessor(nextInChain, name, StringUtils.defaultString(groupName));
			nextInChain = statisticsCompleteProcessor;
			pipeline.setTimer(statisticsCompleteProcessor.getTimer());
		}
		if (asynchronous)
		{
			nextInChain = new AsynchProcessor(nextInChain, threadCount, name, maxQueueSize);
		}
		if (statistics)
		{
			nextInChain = new StatisticsStartProcessor(nextInChain);
		}
		pipeline.setDelegate(nextInChain);
		return pipeline;
	}

	private void verifyBuild()
	{
		if (delegateProcessor == null)
		{
			throw new IllegalStateException("delegateProcessor must be set");
		}
		if (name == null)
		{
			throw new IllegalStateException("name must be set");
		}
		if (asynchronous)
		{
			if (threadCount <= 0)
			{
				throw new IllegalStateException("threadCount must be positive for asynchronous");
			}
			if (maxQueueSize <= 0)
			{
				throw new IllegalStateException("maxQueueSize must be positive for asynchronous");
			}
		}
	}
}
