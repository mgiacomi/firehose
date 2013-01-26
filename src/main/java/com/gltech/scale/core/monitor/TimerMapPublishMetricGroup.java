package com.gltech.scale.core.monitor;

import ganglia.gmetric.GMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class TimerMapPublishMetricGroup implements PublishMetricGroup
{
	private static final Logger logger = LoggerFactory.getLogger(TimerMapPublishMetricGroup.class);

	private PublishMetric metric;
	private TimerMap timerMap;
	private Map<String, TimerCountPublisher> countPublisherMap = new HashMap<>();
	private Map<String, TimerAveragePublisher> averagePublisherMap = new HashMap<>();

	public TimerMapPublishMetricGroup(String groupName, TimerMap timerMap)
	{
		this.metric = new PublishMetric("NA", groupName, "NA", null);
		this.timerMap = timerMap;
	}

	public void publishMetric(GMetric gMetric)
	{
		try
		{
			Collection<Timer> timers = timerMap.getTimers();
			for (Timer timer : timers)
			{
				String name = timer.getName();
				TimerCountPublisher countPublisher = countPublisherMap.get(name);
				if (null == countPublisher)
				{
					countPublisher = new TimerCountPublisher(name, timer);
					countPublisherMap.put(name, countPublisher);
				}
				MonitoringPublisher.publish(gMetric, countPublisher, name + ".Count", "count",
						metric.getType(), metric.getSlope(), metric.getGroupName());

				TimerAveragePublisher averagePublisher = averagePublisherMap.get(name);
				if (null == averagePublisher)
				{
					averagePublisher = new TimerAveragePublisher(name, timer);
					averagePublisherMap.put(name, averagePublisher);
				}
				MonitoringPublisher.publish(gMetric, averagePublisher, name + ".Time", "millis per call",
						metric.getType(), metric.getSlope(), metric.getGroupName());
			}
		}
		catch (Exception e)
		{
			logger.error("unable to publish ", e);
		}

	}

	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		TimerMapPublishMetricGroup that = (TimerMapPublishMetricGroup) o;

		if (!metric.equals(that.metric)) return false;

		return true;
	}

	public int hashCode()
	{
		return metric.hashCode();
	}
}
