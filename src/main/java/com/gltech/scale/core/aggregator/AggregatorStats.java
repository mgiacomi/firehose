package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.model.BatchMetaData;
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
	private AvgStatOverTime batchCollectTimeStat;
	private AvgStatOverTime messagesCollectedPerBatchStat;
	private AvgStatOverTime backupBatchCollectTimeStat;
	private AvgStatOverTime backupMessagesCollectedPerBatchStat;

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
		this.batchCollectTimeStat = statsManager.createAvgStat(groupName, "BatchCollect_Time", "milliseconds");
		this.messagesCollectedPerBatchStat = statsManager.createAvgStat(groupName, "MessagesCollectedPerBatch_Avg", "messages");
		this.backupBatchCollectTimeStat = statsManager.createAvgStat(groupName, "BackupBatchCollect_Time", "milliseconds");
		this.backupMessagesCollectedPerBatchStat = statsManager.createAvgStat(groupName, "BackupMessagesCollectedPerBatch_Avg", "messages");
		this.clearCountStat = statsManager.createCountStat(groupName, "Clear_Count", "calls");
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
		statsManager.createAvgStat(groupName, "TotalBackupQueueSize_Avg", "bytes", new StatCallBack()
		{
			public long getValue()
			{
				long total = 0;
				for (Batch batch : aggregator.getActiveBackupBatches())
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
					total += batch.getMessages();
				}
				return total;
			}
		});
		statsManager.createAvgStat(groupName, "BackupMessagesInQueue_Avg", "messages", new StatCallBack()
		{
			public long getValue()
			{
				long total = 0;
				for (Batch batch : aggregator.getActiveBackupBatches())
				{
					total += batch.getMessages();
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
		statsManager.createAvgStat(groupName, "OldestBackupInQueue_Avg", "seconds", new StatCallBack()
		{
			public long getValue()
			{
				DateTime oldest = new DateTime();
				for (Batch batch : aggregator.getActiveBackupBatches())
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

	@Override
	public void addMessage(String channelName, byte[] bytes, DateTime nearestPeriodCeiling)
	{
		addMessageTimeStat.startTimer();
		aggregator.addMessage(channelName, bytes, nearestPeriodCeiling);
		addMessageTimeStat.stopTimer();

		addMessageSizeStat.add(bytes.length);
	}

	@Override
	public void addBackupMessage(String channelName, byte[] bytes, DateTime nearestPeriodCeiling)
	{
		addBackupMessageTimeStat.startTimer();
		aggregator.addBackupMessage(channelName, bytes, nearestPeriodCeiling);
		addBackupMessageTimeStat.stopTimer();

		addBackupMessageSizeStat.add(bytes.length);
	}

	@Override
	public void clear(String channelName, DateTime dateTime)
	{
		aggregator.clear(channelName, dateTime);
		clearCountStat.increment();
	}

	@Override
	public List<Batch> getActiveBatches()
	{
		return aggregator.getActiveBatches();
	}

	@Override
	public List<Batch> getActiveBackupBatches()
	{
		return aggregator.getActiveBackupBatches();
	}

	@Override
	public long writeBatchMessages(OutputStream outputStream, String channelName, DateTime dateTime)
	{
		batchCollectTimeStat.startTimer();
		long processed = aggregator.writeBatchMessages(outputStream, channelName, dateTime);
		batchCollectTimeStat.stopTimer();
		messagesCollectedPerBatchStat.add(processed);
		return processed;
	}

	@Override
	public long writeBackupBatchMessages(OutputStream outputStream, String channelName, DateTime dateTime)
	{
		backupBatchCollectTimeStat.startTimer();
		long processed = aggregator.writeBackupBatchMessages(outputStream, channelName, dateTime);
		backupBatchCollectTimeStat.stopTimer();
		backupMessagesCollectedPerBatchStat.add(processed);
		return processed;
	}

	@Override
	public BatchMetaData getBatchMetaData(String channelName, DateTime dateTime)
	{
		return aggregator.getBatchMetaData(channelName, dateTime);
	}

	@Override
	public BatchMetaData getBatchBucketMetaData(String channelName, DateTime dateTime)
	{
		return aggregator.getBatchBucketMetaData(channelName, dateTime);
	}

	@Override
	public void shutdown()
	{
		aggregator.shutdown();
	}
}
