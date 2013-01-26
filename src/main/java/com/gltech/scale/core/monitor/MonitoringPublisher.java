package com.gltech.scale.core.monitor;


import com.gltech.scale.core.lifecycle.LifeCycle;
import com.gltech.scale.core.lifecycle.LifeCycleManager;
import com.gltech.scale.core.util.Props;
import ganglia.gmetric.GMetric;
import ganglia.gmetric.GMetricSlope;
import ganglia.gmetric.GMetricType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.*;

/**
 * The MonitoringPublisher keeps track of what should be published to Ganglia
 * and made available to JMX.
 * Register a PublishMetric for a single metric, or use a PublishMetricGroup for a group
 * of related metrics.
 */
public class MonitoringPublisher implements LifeCycle
{
	private static final Logger logger = LoggerFactory.getLogger(MonitoringPublisher.class);

	private static Set<PublishMetric> publishMetrics = Collections.newSetFromMap(new ConcurrentHashMap<PublishMetric, Boolean>());
	private static Set<PublishMetricGroup> publishMetricGroups = Collections.newSetFromMap(new ConcurrentHashMap<PublishMetricGroup, Boolean>());

	private static MonitoringPublisher instance = new MonitoringPublisher();
	private ScheduledExecutorService scheduledService;

	private MonitoringPublisher()
	{
		scheduledService = Executors.newScheduledThreadPool(1, new ThreadFactory()
		{
			public Thread newThread(Runnable runnable)
			{
				return new Thread(runnable, "MonitoringPublisher");
			}
		});
		scheduledService.scheduleAtFixedRate(new Runnable()
		{
			public void run()
			{
				Props props = Props.getProps();
				String ipAddress = props.get("gangliaIpaddress", "localhost");
				int port = props.get("gangliaPort", 8649);
				GMetric gMetric = new GMetric(ipAddress, port, GMetric.UDPAddressingMode.UNICAST, true);
				for (PublishMetric metric : publishMetrics)
				{
					publishMetric(gMetric, metric);
				}
				for (PublishMetricGroup publishMetricGroup : publishMetricGroups)
				{
					publishMetricGroup.publishMetric(gMetric);
				}
			}
		}, 60, 60, TimeUnit.SECONDS);
		LifeCycleManager.getInstance().add(this, LifeCycle.Priority.INITIAL);
	}

	private void publishMetric(GMetric gMetric, PublishMetric metric)
	{
		publish(gMetric, metric.getValueCallback(), metric.getName(),
				metric.getUnits(), metric.getType(), metric.getSlope(), metric.getGroupName());
	}

	public static void publish(GMetric gMetric, PublishCallback callback, String name, String units,
							   GMetricType type, GMetricSlope slope, String groupName)
	{
		//todo - gfm - 10/31/12 - publish to jmx using https://github.com/martint/jmxutils
		try
		{
			String value = callback.getValue();
			logger.info("publishing " + name + " " + value);
			gMetric.announce(name, value, type,
					units, slope, 60, 1440 * 60, groupName);
		}
		catch (Exception e)
		{
			logger.warn("unable to publish " + name, e);
		}
	}

	public static synchronized MonitoringPublisher getInstance()
	{
		return instance;
	}

	public void register(PublishMetric publishMetric)
	{
		publishMetrics.add(publishMetric);
	}

	public void register(PublishMetricGroup publishMetricGroup)
	{
		publishMetricGroups.add(publishMetricGroup);
	}

	public void shutdown()
	{
		scheduledService.shutdownNow();
	}
}
