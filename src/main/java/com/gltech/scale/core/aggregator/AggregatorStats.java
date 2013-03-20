package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.model.BatchMetaData;
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
	private Timer addMessageTimer = new Timer();
	private Timer addBackupMessageTimer = new Timer();
	private Timer clearTimer = new Timer();
	private Timer writtenMessagesTimer = new Timer();
	private Timer writtenBackupMessagesTimer = new Timer();

	@Inject
	public AggregatorStats(@Named(BASE) final Aggregator aggregator)
	{
		this.aggregator = aggregator;

		String groupName = "Aggregator";
		MonitoringPublisher.getInstance().register(new PublishMetric("AddMessage.Count", groupName, "count", new TimerCountPublisher("", addMessageTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric("AddMessage.AvgSize", groupName, "avg payload size bytes", new TimerAveragePublisher("", addMessageTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric("AddBackupMessage.Count", groupName, "count", new TimerCountPublisher("", addBackupMessageTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric("AddBackupMessage.AvgSize", groupName, "avg payload size bytes", new TimerAveragePublisher("", addBackupMessageTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric("Clear.Count", groupName, "count", new TimerCountPublisher("", clearTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric("Clear.Time", groupName, "millis per call", new TimerAveragePublisher("", clearTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric("MessagesWritten.Count", groupName, "count", new TimerCountPublisher("", writtenMessagesTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric("MessagesWritten.Time", groupName, "millis per call", new TimerAveragePublisher("", writtenMessagesTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric("BackupMessagesWritten.Count", groupName, "count", new TimerCountPublisher("", writtenBackupMessagesTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric("BackupMessagesWritten.Time", groupName, "millis per call", new TimerAveragePublisher("", writtenBackupMessagesTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric("ActiveBatches.Count", groupName, "count", new PublishCallback()
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
		addMessageTimer.add(bytes.length);
	}

	public void addBackupMessage(String channelName, byte[] bytes)
	{
		aggregator.addBackupMessage(channelName, bytes);
		addBackupMessageTimer.add(bytes.length);
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
		writtenMessagesTimer.add(System.nanoTime() - start, processed);
		return processed;
	}

	public long writeBackupBatchMessages(OutputStream outputStream, String channelName, DateTime dateTime)
	{
		long start = System.nanoTime();
		long processed = aggregator.writeBackupBatchMessages(outputStream, channelName, dateTime);
		writtenBackupMessagesTimer.add(System.nanoTime() - start, processed);
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
