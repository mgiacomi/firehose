package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.model.Message;
import com.gltech.scale.core.model.Batch;
import com.gltech.scale.core.model.ChannelMetaData;
import org.joda.time.DateTime;

import java.util.Collection;

public interface Channel
{
	ChannelMetaData getChannelMetaData();

	void addEvent(Message message);

	void addBackupEvent(Message message);

	Collection<Batch> getTimeBuckets();

	Collection<Batch> getBackupTimeBuckets();

	Batch getTimeBucket(DateTime nearestPeriodCeiling);

	Batch getBackupTimeBucket(DateTime nearestPeriodCeiling);

	void clear(DateTime nearestPeriodCeiling);

	void clearBackup(DateTime nearestPeriodCeiling);
}
