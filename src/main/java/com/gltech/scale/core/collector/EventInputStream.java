package com.gltech.scale.core.collector;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.gltech.scale.core.event.EventPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class EventInputStream implements EventStream
{
	private static final Logger logger = LoggerFactory.getLogger("com.lokiscale.collector.EventInputStream");
	private final JsonParser jp;
	private EventPayload currentEventPayload;
	private boolean reachedEndArray = false;
	private final InputStream inputStream;
	private int counter = 0;
	private String customerBucketPeriod;

	public EventInputStream(String customerBucketPeriod, InputStream inputStream)
	{
		this.customerBucketPeriod = customerBucketPeriod;
		this.inputStream = inputStream;

		try
		{
			JsonFactory f = new MappingJsonFactory();
			jp = f.createJsonParser(inputStream);
			jp.nextToken();
			jp.nextToken();
		}
		catch (IOException e)
		{
			logger.warn("unable to parse json " + customerBucketPeriod, e);
			throw new RuntimeException("unable to parse json " + customerBucketPeriod, e);
		}
	}

	public EventPayload getCurrentEventPayload()
	{
		if (currentEventPayload == null)
		{
			nextRecord();
		}

		return currentEventPayload;
	}

	public void nextRecord()
	{
		if (!reachedEndArray)
		{
			try
			{
				currentEventPayload = EventPayload.jsonToEvent(jp);

				if (jp.nextToken() == JsonToken.END_ARRAY)
				{
					reachedEndArray = true;
				}
			}
			catch (IOException e)
			{
				logger.warn("unable to parse json " + customerBucketPeriod, e);
				throw new RuntimeException("unable to parse json " + customerBucketPeriod, e);
			}

			counter++;
		}
		else
		{
			currentEventPayload = null;
		}
	}

	public void close()
	{
		try
		{
			logger.debug("Closing Stream (Input) processed {} events for {}", counter, customerBucketPeriod);
			jp.close();
			inputStream.close();

		}
		catch (IOException e)
		{
			// nothing we can do about it at this point
		}
	}
}
