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
	void addEvent(Message message);

	void addBackupEvent(Message message);

	void clear(String customer, String bucket, DateTime dateTime);

	List<Batch> getActiveTimeBuckets();

	List<Batch> getActiveBackupTimeBuckets();

	long writeTimeBucketEvents(OutputStream outputStream, String customer, String bucket, DateTime dateTime);

	long writeBackupTimeBucketEvents(OutputStream outputStream, String customer, String bucket, DateTime dateTime);

	BatchMetaData getTimeBucketMetaData(String customer, String bucket, DateTime dateTime);

	BatchMetaData getBackupTimeBucketMetaData(String customer, String bucket, DateTime dateTime);
}
