package com.gltech.scale.pipeline;

import com.gltech.scale.ganglia.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class PipelineImpl<T> implements Pipeline<T>
{
	private static final Logger logger = LoggerFactory.getLogger(PipelineImpl.class);

	private Processor<T> delegate;
	private String name;
	private Timer timer;

	public String getName()
	{
		return name;
	}

	public long getCount()
	{
		return timer.getCount();
	}

	public double getAverageTimeMillis()
	{
		return timer.getAverage();
	}

	public void process(T t) throws Exception
	{
		delegate.process(t);
	}

	public void setDelegate(Processor<T> delegate)
	{
		this.delegate = delegate;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public void setTimer(Timer timer)
	{
		this.timer = timer;
	}
}
