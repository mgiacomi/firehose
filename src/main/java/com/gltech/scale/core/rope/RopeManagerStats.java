package com.gltech.scale.core.rope;

import com.gltech.scale.core.model.Message;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.gltech.scale.core.monitor.*;
import org.joda.time.DateTime;

import java.io.OutputStream;
import java.util.List;

public class RopeManagerStats implements RopeManager
{
	public static final String BASE = "RopeManagerStats";

	private final RopeManager ropeManager;
	private Timer addEventTimer = new Timer();
	private Timer addBackupEventTimer = new Timer();
	private Timer clearTimer = new Timer();
	private Timer writtenEventsTimer = new Timer();
	private Timer writtenBackupEventsTimer = new Timer();

	@Inject
	public RopeManagerStats(@Named(BASE) final RopeManager ropeManager)
	{
		this.ropeManager = ropeManager;

		String groupName = "Loki Rope Manager";
		MonitoringPublisher.getInstance().register(new PublishMetric("AddEvent.Count", groupName, "count", new TimerCountPublisher("", addEventTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric("AddEvent.AvgSize", groupName, "avg payload size bytes", new TimerAveragePublisher("", addEventTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric("AddBackupEvent.Count", groupName, "count", new TimerCountPublisher("", addBackupEventTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric("AddBackupEvent.AvgSize", groupName, "avg payload size bytes", new TimerAveragePublisher("", addBackupEventTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric("Clear.Count", groupName, "count", new TimerCountPublisher("", clearTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric("Clear.Time", groupName, "millis per call", new TimerAveragePublisher("", clearTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric("EventsWritten.Count", groupName, "count", new TimerCountPublisher("", writtenEventsTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric("EventsWritten.Time", groupName, "millis per call", new TimerAveragePublisher("", writtenEventsTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric("BackupEventsWritten.Count", groupName, "count", new TimerCountPublisher("", writtenBackupEventsTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric("BackupEventsWritten.Time", groupName, "millis per call", new TimerAveragePublisher("", writtenBackupEventsTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric("ActiveTimeBuckets.Count", groupName, "count", new PublishCallback()
		{
			public String getValue()
			{
				return Integer.toString(ropeManager.getActiveTimeBuckets().size());
			}
		}));

	}

	public void addEvent(Message message)
	{
		ropeManager.addEvent(message);
		addEventTimer.add(message.getPayload().length);
	}

	public void addBackupEvent(Message message)
	{
		ropeManager.addBackupEvent(message);
		addBackupEventTimer.add(message.getPayload().length);
	}

	public void clear(String customer, String bucket, DateTime dateTime)
	{
		clearTimer.start();
		ropeManager.clear(customer, bucket, dateTime);
		clearTimer.stop();
	}

	public List<TimeBucket> getActiveTimeBuckets()
	{
		return ropeManager.getActiveTimeBuckets();
	}

	public List<TimeBucket> getActiveBackupTimeBuckets()
	{
		return ropeManager.getActiveBackupTimeBuckets();
	}

	public long writeTimeBucketEvents(OutputStream outputStream, String customer, String bucket, DateTime dateTime)
	{
		long start = System.nanoTime();
		long processed = ropeManager.writeTimeBucketEvents(outputStream, customer, bucket, dateTime);
		writtenEventsTimer.add(System.nanoTime() - start, processed);
		return processed;
	}

	public long writeBackupTimeBucketEvents(OutputStream outputStream, String customer, String bucket, DateTime dateTime)
	{
		long start = System.nanoTime();
		long processed = ropeManager.writeBackupTimeBucketEvents(outputStream, customer, bucket, dateTime);
		writtenBackupEventsTimer.add(System.nanoTime() - start, processed);
		return processed;
	}

	public TimeBucketMetaData getTimeBucketMetaData(String customer, String bucket, DateTime dateTime)
	{
		return ropeManager.getTimeBucketMetaData(customer, bucket, dateTime);
	}

	public TimeBucketMetaData getBackupTimeBucketMetaData(String customer, String bucket, DateTime dateTime)
	{
		return ropeManager.getBackupTimeBucketMetaData(customer, bucket, dateTime);
	}

	public void shutdown()
	{
		ropeManager.shutdown();
	}
}
