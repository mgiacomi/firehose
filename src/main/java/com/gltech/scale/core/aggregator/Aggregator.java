package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.model.BatchMetaData;
import com.gltech.scale.core.model.Message;
import com.gltech.scale.core.model.Batch;
import com.gltech.scale.lifecycle.LifeCycle;
import org.joda.time.DateTime;

import java.io.OutputStream;
import java.util.List;

public interface Aggregator extends LifeCycle
{
	void addMessage(String channelName, byte[] bytes, DateTime nearestPeriodCeiling);

	void addBackupMessage(String channelName, byte[] bytes, DateTime nearestPeriodCeiling);

	void clear(String channelName, DateTime dateTime);

	List<Batch> getActiveBatches();

	List<Batch> getActiveBackupBatches();

	long writeBatchMessages(OutputStream outputStream, String channel, DateTime dateTime);

	long writeBackupBatchMessages(OutputStream outputStream, String channel, DateTime dateTime);

	BatchMetaData getBatchMetaData(String channelName, DateTime dateTime);

	BatchMetaData getBatchBucketMetaData(String channelName, DateTime dateTime);
}
