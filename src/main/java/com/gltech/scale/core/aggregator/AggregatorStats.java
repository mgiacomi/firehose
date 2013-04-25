package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.model.BatchMetaData;
import com.gltech.scale.core.model.Batch;
import com.gltech.scale.core.stats.AvgStatOverTime;
import com.gltech.scale.core.stats.CountStatOverTime;
import com.gltech.scale.core.stats.StatCallBack;
import com.gltech.scale.core.stats.StatsManager;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.joda.time.DateTime;
import org.joda.time.Seconds;

import java.io.OutputStream;
import java.util.List;

public class AggregatorStats implements Aggregator
{
	public static final String BASE = "AggregatorStats";

	private final Aggregator aggregator;
	private AvgStatOverTime addMessageSizeStat;
	private AvgStatOverTime addMessageTimeStat;
	private AvgStatOverTime addBackupMessageSizeStat;
	private AvgStatOverTime addBackupMessageTimeStat;
	private CountStatOverTime clearCountStat;
	private AvgStatOverTime messagesWrittenTimeStat;
	private CountStatOverTime messagesWrittenCountStat;
	private AvgStatOverTime backupMessagesWrittenTimeStat;
	private CountStatOverTime backupMessagesWrittenCountStat;

	@Inject
	public AggregatorStats(@Named(BASE) final Aggregator aggregator, StatsManager statsManager)
	{
		this.aggregator = aggregator;

		String groupName = "Aggregator";
		this.addMessageSizeStat = statsManager.createAvgStat(groupName, "AddMessage_Size", "bytes");
		this.addMessageSizeStat.activateCountStat("AddMessage_Count", "messages");
		this.addMessageTimeStat = statsManager.createAvgStat(groupName, "AddMessage_Time", "milliseconds");
		this.addBackupMessageSizeStat = statsManager.createAvgStat(groupName, "AddBackupMessage_Size", "bytes");
		this.addBackupMessageSizeStat.activateCountStat("AddBackupMessage_Count", "messages");
		this.addBackupMessageTimeStat = statsManager.createAvgStat(groupName, "AddBackupMessage_Time", "milliseconds");
		this.messagesWrittenTimeStat = statsManager.createAvgStat(groupName, "MessagesWritten_Time", "milliseconds");
		this.messagesWrittenCountStat = statsManager.createCounterStat(groupName, "MessagesWritten_Count", "messages");
		this.backupMessagesWrittenTimeStat = statsManager.createAvgStat(groupName, "BackupMessagesWritten_Time", "milliseconds");
		this.backupMessagesWrittenCountStat = statsManager.createCounterStat(groupName, "BackupMessagesWritten_Count", "messages");
		this.clearCountStat = statsManager.createCounterStat(groupName, "Clear_Count", "calls");
		statsManager.createAvgStat(groupName, "ActiveBatches_Avg", "batches", new StatCallBack()
		{
			public long getValue()
			{
				return aggregator.getActiveBatches().size();
			}
		});
		statsManager.createAvgStat(groupName, "ActiveBackupBatches_Avg", "batches", new StatCallBack()
		{
			public long getValue()
			{
				return aggregator.getActiveBackupBatches().size();
			}
		});
		statsManager.createAvgStat(groupName, "TotalQueueSize_Avg", "bytes", new StatCallBack()
		{
			public long getValue()
			{
				long total = 0;
				for (Batch batch : aggregator.getActiveBatches())
				{
					total += batch.getBytes();
				}
				return total;
			}
		});
		statsManager.createAvgStat(groupName, "MessagesInQueue_Avg", "messages", new StatCallBack()
		{
			public long getValue()
			{
				long total = 0;
				for (Batch batch : aggregator.getActiveBatches())
				{
					total += batch.getMessages().size();
				}
				return total;
			}
		});
		statsManager.createAvgStat(groupName, "OldestInQueue_Avg", "seconds", new StatCallBack()
		{
			public long getValue()
			{
				DateTime oldest = new DateTime();
				for (Batch batch : aggregator.getActiveBatches())
				{
					if (batch.getFirstMessageTime().isBefore(oldest))
					{
						oldest = batch.getFirstMessageTime();
					}
				}
				return Seconds.secondsBetween(oldest, new DateTime()).getSeconds();
			}
		});
	}

	public void addMessage(String channelName, byte[] bytes)
	{
		addMessageTimeStat.startTimer();
		aggregator.addMessage(channelName, bytes);
		addMessageTimeStat.stopTimer();

		addMessageSizeStat.add(bytes.length);
	}

	public void addBackupMessage(String channelName, byte[] bytes)
	{
		addBackupMessageTimeStat.startTimer();
		aggregator.addBackupMessage(channelName, bytes);
		addBackupMessageTimeStat.stopTimer();

		addBackupMessageSizeStat.add(bytes.length);
	}

	public void clear(String channelName, DateTime dateTime)
	{
		aggregator.clear(channelName, dateTime);
		clearCountStat.increment();
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
		messagesWrittenTimeStat.startTimer();
		long processed = aggregator.writeBatchMessages(outputStream, channelName, dateTime);
		messagesWrittenTimeStat.stopTimer();
		messagesWrittenCountStat.add(processed);
		return processed;
	}

	public long writeBackupBatchMessages(OutputStream outputStream, String channelName, DateTime dateTime)
	{
		backupMessagesWrittenTimeStat.startTimer();
		long processed = aggregator.writeBackupBatchMessages(outputStream, channelName, dateTime);
		backupMessagesWrittenTimeStat.stopTimer();
		backupMessagesWrittenCountStat.add(processed);
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
