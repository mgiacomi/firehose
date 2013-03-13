package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.model.Message;
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

		String prefix = channel.getChannelMetaData().getCustomer() + "/" + channel.getChannelMetaData().getBucket() + "/" + props.get("coordination.period_seconds", 5);
		String groupName = "Channel (" + prefix + ")";
		MonitoringPublisher.getInstance().register(new PublishMetric(prefix + " AddEvent.Count", groupName, "count", new TimerCountPublisher("", addEventTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric(prefix + " AddEvent.AvgSize", groupName, "avg payload size bytes", new TimerAveragePublisher("", addEventTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric(prefix + " OldestEvent.Time", groupName, "oldest event seconds", new PublishCallback()
		{
			public String getValue()
			{
				DateTime firstEventTime = null;

				for (Batch batch : channel.getTimeBuckets())
				{
					if (firstEventTime == null)
					{
						firstEventTime = batch.getFirstEventTime();
					}

					if (batch.getFirstEventTime().isBefore(firstEventTime))
					{
						firstEventTime = batch.getFirstEventTime();
					}

				}

				if (firstEventTime == null)
				{
					return "0";
				}

				return Long.toString(System.currentTimeMillis() - firstEventTime.getMillis() / 1000);
			}
		}));
		MonitoringPublisher.getInstance().register(new PublishMetric(prefix + " TimeBuckets.Count", groupName, "count", new PublishCallback()
		{
			public String getValue()
			{
				return Integer.toString(channel.getTimeBuckets().size());
			}
		}));
		MonitoringPublisher.getInstance().register(new PublishMetric(prefix + " TimeBuckets.Size", groupName, "size in kb", new PublishCallback()
		{
			public String getValue()
			{
				long channelPayloadSize = 0;

				for (Batch batch : channel.getTimeBuckets())
				{
					channelPayloadSize += batch.getBytes();
				}

				return Long.toString(channelPayloadSize / KBytes);
			}
		}));

		if (channel.getChannelMetaData().getRedundancy() == ChannelMetaData.Redundancy.doublewritesync)
		{
			MonitoringPublisher.getInstance().register(new PublishMetric(prefix + " AddBackupEvent.Count", groupName, "count", new TimerCountPublisher("", addBackupEventTimer)));
			MonitoringPublisher.getInstance().register(new PublishMetric(prefix + " AddBackupEvent.AvgSize", groupName, "avg payload size bytes", new TimerAveragePublisher("", addBackupEventTimer)));
			MonitoringPublisher.getInstance().register(new PublishMetric(prefix + " BackupTimeBuckets.Count", groupName, "count", new PublishCallback()
			{
				public String getValue()
				{
					return Integer.toString(channel.getBackupTimeBuckets().size());
				}
			}));
			MonitoringPublisher.getInstance().register(new PublishMetric(prefix + " BackupTimeBuckets.Size", groupName, "size in kb", new PublishCallback()
			{
				public String getValue()
				{
					long channelPayloadSize = 0;

					for (Batch batch : channel.getBackupTimeBuckets())
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

	public void addEvent(Message message)
	{
		channel.addEvent(message);
		addEventTimer.add(message.getPayload().length);
	}

	public void addBackupEvent(Message message)
	{
		channel.addBackupEvent(message);
		addBackupEventTimer.add(message.getPayload().length);
	}

	public Collection<Batch> getTimeBuckets()
	{
		return channel.getTimeBuckets();
	}

	public Collection<Batch> getBackupTimeBuckets()
	{
		return channel.getBackupTimeBuckets();
	}

	public Batch getTimeBucket(DateTime nearestPeriodCeiling)
	{
		return channel.getTimeBucket(nearestPeriodCeiling);
	}

	public Batch getBackupTimeBucket(DateTime nearestPeriodCeiling)
	{
		return channel.getBackupTimeBucket(nearestPeriodCeiling);
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
