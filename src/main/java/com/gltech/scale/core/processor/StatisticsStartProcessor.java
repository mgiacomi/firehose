package com.gltech.scale.core.processor;

/**
 *
 */
public class StatisticsStartProcessor<T extends Timed> implements Processor<T>
{
	private Processor<T> delegate;

	public StatisticsStartProcessor(Processor<T> delegate)
	{
		this.delegate = delegate;
	}

	public void process(T timed) throws Exception
	{
		timed.start();
		delegate.process(timed);
	}
}
