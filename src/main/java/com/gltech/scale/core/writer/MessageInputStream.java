package com.gltech.scale.core.writer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.gltech.scale.core.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class MessageInputStream implements MessageStream
{
	private static final Logger logger = LoggerFactory.getLogger(MessageInputStream.class);
	private final JsonParser jp;
	private Message currentMessage;
	private boolean reachedEndArray = false;
	private final InputStream inputStream;
	private int counter = 0;
	private String customerBucketPeriod;

	public MessageInputStream(String customerBucketPeriod, InputStream inputStream)
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

	public Message getCurrentMessage()
	{
		if (currentMessage == null)
		{
			nextRecord();
		}

		return currentMessage;
	}

	public void nextRecord()
	{
		if (!reachedEndArray)
		{
			try
			{
				currentMessage = Message.jsonToEvent(jp);

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
			currentMessage = null;
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
