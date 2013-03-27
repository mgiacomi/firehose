package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.model.Batch;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.stats.AvgStatOverTime;
import com.gltech.scale.core.stats.StatCallBack;
import com.gltech.scale.core.stats.StatsManager;
import com.gltech.scale.util.Props;
import org.joda.time.DateTime;

import java.util.Collection;

public class ChannelStats implements Channel
{
	private static final long KBytes = 1024L;
	private Props props = Props.getProps();
	private final Channel channel;
	private AvgStatOverTime addMessageSizeStat;
	private AvgStatOverTime addBackupMessageSizeStat;

	public ChannelStats(final Channel channel, StatsManager statsManager)
	{
		this.channel = channel;

		String groupName = "Channel (" + channel.getChannelMetaData().getName() + ")";
		this.addMessageSizeStat = statsManager.createAvgAndCountStat(groupName, "AddMessage.Size", "AddMessage.Count");

		statsManager.createAvgStat(groupName, "OldestMessage.Time", new StatCallBack()
		{
			public long getValue()
			{
				DateTime firstMessageTime = null;

				for (Batch batch : channel.getBatches())
				{
					if (firstMessageTime == null)
					{
						firstMessageTime = batch.getFirstMessageTime();
					}

					if (batch.getFirstMessageTime().isBefore(firstMessageTime))
					{
						firstMessageTime = batch.getFirstMessageTime();
					}

				}

				if (firstMessageTime == null)
				{
					return 0;
				}

				return System.currentTimeMillis() - firstMessageTime.getMillis() / 1000;
			}
		});

		statsManager.createCounterStat(groupName, "Batches.Count", new StatCallBack()
		{
			public long getValue()
			{
				return channel.getBatches().size();
			}
		});

		statsManager.createCounterStat(groupName, "Batches.Size", new StatCallBack()
		{
			public long getValue()
			{
				long channelPayloadSize = 0;

				for (Batch batch : channel.getBatches())
				{
					channelPayloadSize += batch.getBytes();
				}

				return channelPayloadSize / KBytes;
			}
		});

		if (channel.getChannelMetaData().isRedundant())
		{
			this.addBackupMessageSizeStat = statsManager.createAvgAndCountStat(groupName, "AddBackupMessage.Size", "AddBackupMessage.Count");

			statsManager.createCounterStat(groupName, "BackupBatches.Count", new StatCallBack()
			{
				public long getValue()
				{
					return channel.getBackupBatches().size();
				}
			});

			statsManager.createCounterStat(groupName, "BackupBatches.Size", new StatCallBack()
			{
				public long getValue()
				{
					long channelPayloadSize = 0;

					for (Batch batch : channel.getBackupBatches())
					{
						channelPayloadSize += batch.getBytes();
					}

					return channelPayloadSize / KBytes;
				}
			});
		}
	}

	public ChannelMetaData getChannelMetaData()
	{
		return channel.getChannelMetaData();
	}

	public void addMessage(byte[] bytes)
	{
		channel.addMessage(bytes);
		addMessageSizeStat.add(bytes.length);
	}

	public void addBackupMessage(byte[] bytes)
	{
		channel.addBackupMessage(bytes);
		addBackupMessageSizeStat.add(bytes.length);
	}

	public Collection<Batch> getBatches()
	{
		return channel.getBatches();
	}

	public Collection<Batch> getBackupBatches()
	{
		return channel.getBackupBatches();
	}

	public Batch getBatch(DateTime nearestPeriodCeiling)
	{
		return channel.getBatch(nearestPeriodCeiling);
	}

	public Batch getBackupBatch(DateTime nearestPeriodCeiling)
	{
		return channel.getBackupBatch(nearestPeriodCeiling);
	}

	public void clear(DateTime nearestPeriodCeiling)
	{
		channel.clear(nearestPeriodCeiling);
	}

	public void clearBackup(DateTime nearestPeriodCeiling)
	{
		channel.clearBackup(nearestPeriodCeiling);
	}
}
