package com.gltech.scale.core.cluster;

import com.gltech.scale.core.aggregator.AggregatorsByPeriod;
import com.gltech.scale.lifecycle.LifeCycle;
import org.joda.time.DateTime;

public interface ChannelCoordinator extends LifeCycle
{
	void start();

	void registerWeight(boolean active, int primaries, int backups, int restedfor);

	AggregatorsByPeriod getAggregatorPeriodMatrix(DateTime nearestPeriodCeiling);
}