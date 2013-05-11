package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.model.Message;
import com.gltech.scale.core.model.Batch;
import com.gltech.scale.core.model.ChannelMetaData;
import org.joda.time.DateTime;

import java.util.Collection;

public interface Channel
{
	ChannelMetaData getChannelMetaData();

	void addMessage(byte[] bytes, DateTime nearestPeriodCeiling);

	void addBackupMessage(byte[] bytes, DateTime nearestPeriodCeiling);

	Collection<Batch> getBatches();

	Collection<Batch> getBackupBatches();

	Batch getBatch(DateTime nearestPeriodCeiling);

	Batch getBackupBatch(DateTime nearestPeriodCeiling);

	void clear(DateTime nearestPeriodCeiling);

	void clearBackup(DateTime nearestPeriodCeiling);
}
