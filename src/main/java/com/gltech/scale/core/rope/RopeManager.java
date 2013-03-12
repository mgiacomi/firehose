package com.gltech.scale.core.rope;

import com.gltech.scale.core.model.Message;
import com.gltech.scale.lifecycle.LifeCycle;
import org.joda.time.DateTime;

import java.io.OutputStream;
import java.util.List;

public interface RopeManager extends LifeCycle
{
	void addEvent(Message message);

	void addBackupEvent(Message message);

	void clear(String customer, String bucket, DateTime dateTime);

	List<TimeBucket> getActiveTimeBuckets();

	List<TimeBucket> getActiveBackupTimeBuckets();

	long writeTimeBucketEvents(OutputStream outputStream, String customer, String bucket, DateTime dateTime);

	long writeBackupTimeBucketEvents(OutputStream outputStream, String customer, String bucket, DateTime dateTime);

	TimeBucketMetaData getTimeBucketMetaData(String customer, String bucket, DateTime dateTime);

	TimeBucketMetaData getBackupTimeBucketMetaData(String customer, String bucket, DateTime dateTime);
}
