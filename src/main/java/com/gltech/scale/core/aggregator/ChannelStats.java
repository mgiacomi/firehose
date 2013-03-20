package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.core.model.Batch;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.ganglia.*;
import com.gltech.scale.util.Props;
import org.joda.time.DateTime;

import java.util.Collection;

public class ChannelStats implements Channel
{
	private static final long KBytes = 1024L;
	private Props props = Props.getProps();
	private final Channel channel;
	private Timer addEventTimer = new Timer();
	private Timer addBackupEventTimer = new Timer();

	public ChannelStats(final Channel channel)
	{
		this.channel = channel;

		String channelName = channel.getChannelMetaData().getName() + "/" + props.get("period_seconds", Defaults.PERIOD_SECONDS);
		String groupName = "Channel (" + channelName + ")";
		MonitoringPublisher.getInstance().register(new PublishMetric(channelName + " AddEvent.Count", groupName, "count", new TimerCountPublisher("", addEventTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric(channelName + " AddEvent.AvgSize", groupName, "avg payload size bytes", new TimerAveragePublisher("", addEventTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric(channelName + " OldestEvent.Time", groupName, "oldest event seconds", new PublishCallback()
		{
			public String getValue()
			{
				DateTime firstEventTime = null;

				for (Batch batch : channel.getBatches())
				{
					if (firstEventTime == null)
					{
						firstEventTime = batch.getFirstMessageTime();
					}

					if (batch.getFirstMessageTime().isBefore(firstEventTime))
					{
						firstEventTime = batch.getFirstMessageTime();
					}

				}

				if (firstEventTime == null)
				{
					return "0";
				}

				return Long.toString(System.currentTimeMillis() - firstEventTime.getMillis() / 1000);
			}
		}));
		MonitoringPublisher.getInstance().register(new PublishMetric(channelName + " Batches.Count", groupName, "count", new PublishCallback()
		{
			public String getValue()
			{
				return Integer.toString(channel.getBatches().size());
			}
		}));
		MonitoringPublisher.getInstance().register(new PublishMetric(channelName + " Batches.Size", groupName, "size in kb", new PublishCallback()
		{
			public String getValue()
			{
				long channelPayloadSize = 0;

				for (Batch batch : channel.getBatches())
				{
					channelPayloadSize += batch.getBytes();
				}

				return Long.toString(channelPayloadSize / KBytes);
			}
		}));

		if (channel.getChannelMetaData().isRedundant())
		{
			MonitoringPublisher.getInstance().register(new PublishMetric(channelName + " AddBackupMessage.Count", groupName, "count", new TimerCountPublisher("", addBackupEventTimer)));
			MonitoringPublisher.getInstance().register(new PublishMetric(channelName + " AddBackupMessage.AvgSize", groupName, "avg payload size bytes", new TimerAveragePublisher("", addBackupEventTimer)));
			MonitoringPublisher.getInstance().register(new PublishMetric(channelName + " BackupBatches.Count", groupName, "count", new PublishCallback()
			{
				public String getValue()
				{
					return Integer.toString(channel.getBackupBatches().size());
				}
			}));
			MonitoringPublisher.getInstance().register(new PublishMetric(channelName + " BackupBatches.Size", groupName, "size in kb", new PublishCallback()
			{
				public String getValue()
				{
					long channelPayloadSize = 0;

					for (Batch batch : channel.getBackupBatches())
					{
						channelPayloadSize += batch.getBytes();
					}

					return Long.toString(channelPayloadSize / KBytes);
				}
			}));
		}
	}

	public ChannelMetaData getChannelMetaData()
	{
		return channel.getChannelMetaData();
	}

	public void addMessage(byte[] bytes)
	{
		channel.addMessage(bytes);
		addEventTimer.add(bytes.length);
	}

	public void addBackupMessage(byte[] bytes)
	{
		channel.addBackupMessage(bytes);
		addBackupEventTimer.add(bytes.length);
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
