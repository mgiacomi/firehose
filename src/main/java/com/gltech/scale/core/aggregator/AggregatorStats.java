package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.model.BatchMetaData;
import com.gltech.scale.core.model.Batch;
import com.gltech.scale.core.stats.AvgStatOverTime;
import com.gltech.scale.core.stats.CounterStatOverTime;
import com.gltech.scale.core.stats.StatCallBack;
import com.gltech.scale.core.stats.StatsManager;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.joda.time.DateTime;

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
	private CounterStatOverTime clearCountStat;
	private AvgStatOverTime messagesWrittenTimeStat;
	private CounterStatOverTime messagesWrittenCountStat;
	private AvgStatOverTime backupMessagesWrittenTimeStat;
	private CounterStatOverTime backupMessagesWrittenCountStat;

	@Inject
	public AggregatorStats(@Named(BASE) final Aggregator aggregator, StatsManager statsManager)
	{
		this.aggregator = aggregator;

		String groupName = "Aggregator";
		this.addMessageSizeStat = statsManager.createAvgAndCountStat(groupName, "AddMessage.Size", "AddMessage.Count");
		this.addMessageTimeStat = statsManager.createAvgStat(groupName, "AddMessage.Time");
		this.addBackupMessageSizeStat = statsManager.createAvgAndCountStat(groupName, "AddBackupMessage.Size", "AddBackupMessage.Count");
		this.addBackupMessageTimeStat = statsManager.createAvgStat(groupName, "AddBackupMessage.Time");
		this.messagesWrittenTimeStat = statsManager.createAvgStat(groupName, "MessagesWritten.Time");
		this.messagesWrittenCountStat = statsManager.createCounterStat(groupName, "MessagesWritten.Count");
		this.backupMessagesWrittenTimeStat = statsManager.createAvgStat(groupName, "BackupMessagesWritten.Time");
		this.backupMessagesWrittenCountStat = statsManager.createCounterStat(groupName, "BackupMessagesWritten.Count");
		this.clearCountStat = statsManager.createCounterStat(groupName, "Clear.Count");
		statsManager.createAvgStat(groupName, "ActiveBatches.Avg", new StatCallBack()
		{
			public long getValue()
			{
				return aggregator.getActiveBatches().size();
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
