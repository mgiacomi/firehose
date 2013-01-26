package com.gltech.scale.core.monitor;

import ganglia.gmetric.GMetric;

/**
 *
 */
public interface PublishMetricGroup
{
	void publishMetric(GMetric gMetric);

}
