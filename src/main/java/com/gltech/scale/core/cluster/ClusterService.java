package com.gltech.scale.core.cluster;

import com.gltech.scale.core.cluster.registration.RegistrationService;
import com.gltech.scale.lifecycle.LifeCycle;
import com.gltech.scale.core.storage.BucketMetaData;
import org.joda.time.DateTime;

public interface ClusterService extends LifeCycle
{
	RegistrationService getRegistrationService();

	BatchPeriodMapper getOldestCollectibleTimeBucket();

	void addTimeBucket(BucketMetaData bucketMetaData, DateTime nearestPeriodCeiling);

	void clearTimeBucketMetaData(BucketMetaData bucketMetaData, DateTime nearestPeriodCeiling);

	void clearCollectorLock(BucketMetaData bucketMetaData, DateTime nearestPeriodCeiling);
}
