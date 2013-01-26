package com.gltech.scale.core.collector;

import com.gltech.scale.core.monitor.Timer;
import com.gltech.scale.core.storage.BucketMetaData;
import org.joda.time.DateTime;

import java.util.concurrent.Callable;

public interface TimeBucketCollector extends Callable<Object>
{
	void assign(BucketMetaData bucketMetaData, DateTime nearestPeriodCeiling);

	void setTimer(Timer timer);
}
