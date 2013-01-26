package com.gltech.scale.core.coordination;

import com.gltech.scale.core.coordination.registration.RegistrationService;
import com.gltech.scale.core.lifecycle.LifeCycle;
import com.gltech.scale.core.storage.BucketMetaData;
import org.joda.time.DateTime;

public interface CoordinationService extends LifeCycle
{
	RegistrationService getRegistrationService();

	BucketPeriodMapper getOldestCollectibleTimeBucket();

	void addTimeBucket(BucketMetaData bucketMetaData, DateTime nearestPeriodCeiling);

	void clearTimeBucketMetaData(BucketMetaData bucketMetaData, DateTime nearestPeriodCeiling);

	void clearCollectorLock(BucketMetaData bucketMetaData, DateTime nearestPeriodCeiling);
}
