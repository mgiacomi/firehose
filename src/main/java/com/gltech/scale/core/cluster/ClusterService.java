package com.gltech.scale.core.cluster;

import com.gltech.scale.core.cluster.registration.RegistrationService;
import com.gltech.scale.lifecycle.LifeCycle;
import com.gltech.scale.core.model.ChannelMetaData;
import org.joda.time.DateTime;

public interface ClusterService extends LifeCycle
{
	RegistrationService getRegistrationService();

	BatchPeriodMapper getOldestCollectibleBatch();

	void registerBatch(ChannelMetaData channelMetaData, DateTime nearestPeriodCeiling);

	void clearBatchMetaData(ChannelMetaData channelMetaData, DateTime nearestPeriodCeiling);

	void clearCollectorLock(ChannelMetaData channelMetaData, DateTime nearestPeriodCeiling);
}
