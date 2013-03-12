package com.gltech.scale.core.writer;

import com.gltech.scale.core.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MessageListStream implements MessageStream
{
	private static final Logger logger = LoggerFactory.getLogger(MessageListStream.class);
	private List<Message> events;
	private int position = 0;
	private String customerBucketPeriod;

	public MessageListStream(String customerBucketPeriod, List<Message> events)
	{
		this.customerBucketPeriod = customerBucketPeriod;
		this.events = events;
	}

	public Message getCurrentMessage()
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
