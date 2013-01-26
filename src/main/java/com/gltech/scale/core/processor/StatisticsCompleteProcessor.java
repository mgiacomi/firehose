package com.gltech.scale.core.processor;

import com.gltech.scale.core.monitor.*;

/**
 *
 */
public class StatisticsCompleteProcessor<T extends Timed> implements Processor<T>
{
	private Processor<T> delegate;
	private Timer totalTimer;
	private final Timer processingTimer;

	public StatisticsCompleteProcessor(Processor<T> delegate, String name, String groupName)
	{
		this.delegate = delegate;
		TimerMap totalTimerMap = new TimerMap();
		TimerMap processingTimerMap = new TimerMap();
		MonitoringPublisher.getInstance().register(
				new PublishMetric(name + "Count", groupName, "count", new TimerCountPublisher(totalTimerMap)));
		MonitoringPublisher.getInstance().register
				(new PublishMetric(name + "TotalTime", groupName, "millis per call", new TimerAveragePublisher(totalTimerMap)));
		totalTimer = totalTimerMap.get("default");
		MonitoringPublisher.getInstance().register
				(new PublishMetric(name + "ProcessingTime", groupName, "millis per call", new TimerAveragePublisher(processingTimerMap)));
		processingTimer = processingTimerMap.get("default");
	}

	public void process(T timed) throws Exception
	{
		try
		{
			long startNanos = System.nanoTime();
			delegate.process(timed);
			processingTimer.add(System.nanoTime() - startNanos);
		}
		finally
		{
			totalTimer.add(timed.stop());
		}
	}

	Timer getTimer()
	{
		return totalTimer;
	}
}
