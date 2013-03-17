package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.model.BatchMetaData;
import com.gltech.scale.core.model.Message;
import com.gltech.scale.core.model.Batch;
import com.gltech.scale.ganglia.*;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.joda.time.DateTime;

import java.io.OutputStream;
import java.util.List;

public class AggregatorStats implements Aggregator
{
	public static final String BASE = "AggregatorStats";

	private final Aggregator aggregator;
	private Timer addEventTimer = new Timer();
	private Timer addBackupEventTimer = new Timer();
	private Timer clearTimer = new Timer();
	private Timer writtenEventsTimer = new Timer();
	private Timer writtenBackupEventsTimer = new Timer();

	@Inject
	public AggregatorStats(@Named(BASE) final Aggregator aggregator)
	{
		this.aggregator = aggregator;

		String groupName = "Aggregator";
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
				return Integer.toString(aggregator.getActiveBatches().size());
			}
		}));

	}

	public void addMessage(String channelName, byte[] bytes)
	{
		aggregator.addMessage(channelName, bytes);
		addEventTimer.add(bytes.length);
	}

	public void addBackupMessage(String channelName, byte[] bytes)
	{
		aggregator.addBackupMessage(channelName, bytes);
		addBackupEventTimer.add(bytes.length);
	}

	public void clear(String channelName, DateTime dateTime)
	{
		clearTimer.start();
		aggregator.clear(channelName, dateTime);
		clearTimer.stop();
	}

	public List<Batch> getActiveBatches()
	{
		return aggregator.getActiveBatches();
	}

	public List<Batch> getActiveBackupBatches()
	{
		return aggregator.getActiveBackupBatches();
	}

	public long writeBatchMessages(OutputStream outputStream, String channelName, DateTime dateTime)
	{
		long start = System.nanoTime();
		long processed = aggregator.writeBatchMessages(outputStream, channelName, dateTime);
		writtenEventsTimer.add(System.nanoTime() - start, processed);
		return processed;
	}

	public long writeBackupBatchMessages(OutputStream outputStream, String channelName, DateTime dateTime)
	{
		long start = System.nanoTime();
		long processed = aggregator.writeBackupBatchMessages(outputStream, channelName, dateTime);
		writtenBackupEventsTimer.add(System.nanoTime() - start, processed);
		return processed;
	}

	public BatchMetaData getBatchMetaData(String channelName, DateTime dateTime)
	{
		return aggregator.getBatchMetaData(channelName, dateTime);
	}

	public BatchMetaData getBatchBucketMetaData(String channelName, DateTime dateTime)
	{
		return aggregator.getBatchBucketMetaData(channelName, dateTime);
	}

	public void shutdown()
	{
		aggregator.shutdown();
	}
}
