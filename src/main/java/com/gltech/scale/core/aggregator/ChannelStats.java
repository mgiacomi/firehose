package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.core.stats.AvgStatOverTime;
import com.gltech.scale.core.stats.StatCallBack;
import com.gltech.scale.core.stats.StatsManager;
import org.joda.time.DateTime;

import java.util.Collection;

public class ChannelStats implements Channel
{
	private final Channel channel;
	private AvgStatOverTime addMessageSizeStat;
	private AvgStatOverTime addBackupMessageSizeStat;

	public ChannelStats(final Channel channel, StatsManager statsManager)
	{
		this.channel = channel;

		String groupName = "Channel (" + channel.getChannelMetaData().getName() + ")";
		this.addMessageSizeStat = statsManager.createAvgStat(groupName, "AddMessage_Size", "bytes");
		this.addMessageSizeStat.activateCountStat("AddMessage_Count", "messages");

		statsManager.createAvgStat(groupName, "OldestMessage_Time", "seconds", new StatCallBack()
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

		statsManager.createCounterStat(groupName, "ActiveBatches_Count", "batches", new StatCallBack()
		{
			public long getValue()
			{
				return channel.getBatches().size();
			}
		});

		statsManager.createCounterStat(groupName, "ActiveBatches_Size", "kb", new StatCallBack()
		{
			public long getValue()
			{
				long channelPayloadSize = 0;

				for (Batch batch : channel.getBatches())
				{
					channelPayloadSize += batch.getBytes();
				}

				return channelPayloadSize / Defaults.KBytes;
			}
		});

		if (channel.getChannelMetaData().isRedundant())
		{
			this.addBackupMessageSizeStat = statsManager.createAvgStat(groupName, "AddBackupMessage_Size", "bytes");
			this.addBackupMessageSizeStat.activateCountStat("AddBackupMessage_Count", "messages");

			statsManager.createCounterStat(groupName, "ActiveBackupBatches_Count", "batches", new StatCallBack()
			{
				public long getValue()
				{
					return channel.getBackupBatches().size();
				}
			});

			statsManager.createCounterStat(groupName, "ActiveBackupBatches_Size", "kb", new StatCallBack()
			{
				public long getValue()
				{
					long channelPayloadSize = 0;

					for (Batch batch : channel.getBackupBatches())
					{
						channelPayloadSize += batch.getBytes();
					}

					return channelPayloadSize / Defaults.KBytes;
				}
			});
		}
	}

	@Override
	public ChannelMetaData getChannelMetaData()
	{
		return channel.getChannelMetaData();
	}

	@Override
	public void addMessage(byte[] bytes, DateTime nearestPeriodCeiling)
	{
		channel.addMessage(bytes, nearestPeriodCeiling);
		addMessageSizeStat.add(bytes.length);
	}

	@Override
	public void addBackupMessage(byte[] bytes, DateTime nearestPeriodCeiling)
	{
		channel.addBackupMessage(bytes, nearestPeriodCeiling);
System.out.println(addBackupMessageSizeStat +" : "+ (bytes == null));
		addBackupMessageSizeStat.add(bytes.length);
	}

	@Override
	public Collection<Batch> getBatches()
	{
		return channel.getBatches();
	}

	@Override
	public Collection<Batch> getBackupBatches()
	{
		return channel.getBackupBatches();
	}

	@Override
	public Batch getBatch(DateTime nearestPeriodCeiling)
	{
		return channel.getBatch(nearestPeriodCeiling);
	}

	@Override
	public Batch getBackupBatch(DateTime nearestPeriodCeiling)
	{
		return channel.getBackupBatch(nearestPeriodCeiling);
	}

	@Override
	public void clear(DateTime nearestPeriodCeiling)
	{
		channel.clear(nearestPeriodCeiling);
	}

	@Override
	public void clearBackup(DateTime nearestPeriodCeiling)
	{
		channel.clearBackup(nearestPeriodCeiling);
	}
}
