package com.gltech.scale.core.rope;

import com.gltech.scale.core.model.Message;
import com.gltech.scale.core.storage.BucketMetaData;
import org.joda.time.DateTime;

import java.util.Collection;

public interface Rope
{
	BucketMetaData getBucketMetaData();

	void addEvent(Message message);

	void addBackupEvent(Message message);

	Collection<TimeBucket> getTimeBuckets();

	Collection<TimeBucket> getBackupTimeBuckets();

	TimeBucket getTimeBucket(DateTime nearestPeriodCeiling);

	TimeBucket getBackupTimeBucket(DateTime nearestPeriodCeiling);

	void clear(DateTime nearestPeriodCeiling);

	void clearBackup(DateTime nearestPeriodCeiling);
}
