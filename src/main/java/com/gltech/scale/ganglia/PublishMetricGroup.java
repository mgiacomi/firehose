package com.gltech.scale.ganglia;

import ganglia.gmetric.GMetric;

/**
 *
 */
public interface PublishMetricGroup
{
	void publishMetric(GMetric gMetric);

}
