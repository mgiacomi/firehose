package com.gltech.scale.core.cluster;

import com.gltech.scale.core.cluster.registration.RegistrationService;
import com.gltech.scale.lifecycle.LifeCycle;
import com.gltech.scale.core.model.ChannelMetaData;
import org.joda.time.DateTime;

import java.util.List;

public interface ClusterService extends LifeCycle
{
	RegistrationService getRegistrationService();

	BatchPeriodMapper getOldestCollectibleBatch();

	List<BatchPeriodMapper> getOrderedActiveBucketList();

	void registerBatch(ChannelMetaData channelMetaData, DateTime nearestPeriodCeiling);

	void clearBatchMetaData(ChannelMetaData channelMetaData, DateTime nearestPeriodCeiling);

	void clearStorageWriterLock(ChannelMetaData channelMetaData, DateTime nearestPeriodCeiling);
}
