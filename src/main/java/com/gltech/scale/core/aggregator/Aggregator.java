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
	void addEvent(String channelName, Message message);

	void addBackupEvent(String channelName, Message message);

	void clear(String channelName, DateTime dateTime);

	List<Batch> getActiveTimeBuckets();

	List<Batch> getActiveBackupTimeBuckets();

	long writeTimeBucketEvents(OutputStream outputStream, String channel, DateTime dateTime);

	long writeBackupTimeBucketEvents(OutputStream outputStream, String channel, DateTime dateTime);

	BatchMetaData getTimeBucketMetaData(String channelName, DateTime dateTime);

	BatchMetaData getBackupTimeBucketMetaData(String channelName, DateTime dateTime);
}
