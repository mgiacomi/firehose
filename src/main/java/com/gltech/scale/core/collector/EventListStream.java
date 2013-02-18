package com.gltech.scale.core.collector;

import com.gltech.scale.core.event.EventPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class EventListStream implements EventStream
{
	private static final Logger logger = LoggerFactory.getLogger("com.lokiscale.collector.EventListStream");
	private List<EventPayload> events;
	private int position = 0;
	private String customerBucketPeriod;

	public EventListStream(String customerBucketPeriod, List<EventPayload> events)
	{
		this.customerBucketPeriod = customerBucketPeriod;
		this.events = events;
	}

	public EventPayload getCurrentEventPayload()
	{
		if (events.size() > position)
		{
			return events.get(position);
		}

		return null;
	}

	public void nextRecord()
	{
		position++;
	}

	public void close()
	{
		logger.debug("Closing Stream (List) processed {} of {} events for {}", position, events.size(), customerBucketPeriod);
	}
}