package com.gltech.scale.core.rope;

import com.gltech.scale.core.event.EventPayload;
import com.gltech.scale.core.monitor.*;
import com.gltech.scale.core.storage.BucketMetaData;
import com.gltech.scale.core.util.Props;
import org.joda.time.DateTime;

import java.util.Collection;

public class RopeStats implements Rope
{
	private static final long KBytes = 1024L;
	private Props props = Props.getProps();
	private final Rope rope;
	private Timer addEventTimer = new Timer();
	private Timer addBackupEventTimer = new Timer();

	public RopeStats(final Rope rope)
	{
		this.rope = rope;

		String prefix = rope.getBucketMetaData().getCustomer() + "/" + rope.getBucketMetaData().getBucket() + "/" + props.get("coordination.period_seconds", 5);
		String groupName = "Loki Rope (" + prefix + ")";
		MonitoringPublisher.getInstance().register(new PublishMetric(prefix + " AddEvent.Count", groupName, "count", new TimerCountPublisher("", addEventTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric(prefix + " AddEvent.AvgSize", groupName, "avg payload size bytes", new TimerAveragePublisher("", addEventTimer)));
		MonitoringPublisher.getInstance().register(new PublishMetric(prefix + " OldestEvent.Time", groupName, "oldest event seconds", new PublishCallback()
		{
			public String getValue()
			{
				DateTime firstEventTime = null;

				for (TimeBucket timeBucket : rope.getTimeBuckets())
				{
					if (firstEventTime == null)
					{
						firstEventTime = timeBucket.getFirstEventTime();
					}

					if (timeBucket.getFirstEventTime().isBefore(firstEventTime))
					{
						firstEventTime = timeBucket.getFirstEventTime();
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
				return Integer.toString(rope.getTimeBuckets().size());
			}
		}));
		MonitoringPublisher.getInstance().register(new PublishMetric(prefix + " TimeBuckets.Size", groupName, "size in kb", new PublishCallback()
		{
			public String getValue()
			{
				long ropePayloadSize = 0;

				for (TimeBucket timeBucket : rope.getTimeBuckets())
				{
					ropePayloadSize += timeBucket.getBytes();
				}

				return Long.toString(ropePayloadSize / KBytes);
			}
		}));

		if (rope.getBucketMetaData().getRedundancy() == BucketMetaData.Redundancy.doublewritesync)
		{
			MonitoringPublisher.getInstance().register(new PublishMetric(prefix + " AddBackupEvent.Count", groupName, "count", new TimerCountPublisher("", addBackupEventTimer)));
			MonitoringPublisher.getInstance().register(new PublishMetric(prefix + " AddBackupEvent.AvgSize", groupName, "avg payload size bytes", new TimerAveragePublisher("", addBackupEventTimer)));
			MonitoringPublisher.getInstance().register(new PublishMetric(prefix + " BackupTimeBuckets.Count", groupName, "count", new PublishCallback()
			{
				public String getValue()
				{
					return Integer.toString(rope.getBackupTimeBuckets().size());
				}
			}));
			MonitoringPublisher.getInstance().register(new PublishMetric(prefix + " BackupTimeBuckets.Size", groupName, "size in kb", new PublishCallback()
			{
				public String getValue()
				{
					long ropePayloadSize = 0;

					for (TimeBucket timeBucket : rope.getBackupTimeBuckets())
					{
						ropePayloadSize += timeBucket.getBytes();
					}

					return Long.toString(ropePayloadSize / KBytes);
				}
			}));
		}
	}

	public BucketMetaData getBucketMetaData()
	{
		return rope.getBucketMetaData();
	}

	public void addEvent(EventPayload eventPayload)
	{
		rope.addEvent(eventPayload);
		addEventTimer.add(eventPayload.getPayload().length);
	}

	public void addBackupEvent(EventPayload eventPayload)
	{
		rope.addBackupEvent(eventPayload);
		addBackupEventTimer.add(eventPayload.getPayload().length);
	}

	public Collection<TimeBucket> getTimeBuckets()
	{
		return rope.getTimeBuckets();
	}

	public Collection<TimeBucket> getBackupTimeBuckets()
	{
		return rope.getBackupTimeBuckets();
	}

	public TimeBucket getTimeBucket(DateTime nearestPeriodCeiling)
	{
		return rope.getTimeBucket(nearestPeriodCeiling);
	}

	public TimeBucket getBackupTimeBucket(DateTime nearestPeriodCeiling)
	{
		return rope.getBackupTimeBucket(nearestPeriodCeiling);
	}

	public void clear(DateTime nearestPeriodCeiling)
	{
		rope.clear(nearestPeriodCeiling);
	}

	public void clearBackup(DateTime nearestPeriodCeiling)
	{
		rope.clearBackup(nearestPeriodCeiling);
	}
}
