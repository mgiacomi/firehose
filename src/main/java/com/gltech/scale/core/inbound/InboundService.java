package com.gltech.scale.core.inbound;

import com.gltech.scale.core.lifecycle.LifeCycle;
import com.gltech.scale.core.storage.BucketMetaData;
import org.joda.time.DateTime;

import java.io.OutputStream;

public interface InboundService extends LifeCycle
{
	void addEvent(String customer, String bucket, byte[] payload);

	int writeEventsToOutputStream(BucketMetaData bucketMetaData, DateTime dateTime, OutputStream outputStream, int recordsWritten);
}
