package com.gltech.scale.core.writer;

import com.gltech.scale.core.model.Message;
import com.gltech.scale.util.ModelIO;
import com.google.protobuf.CodedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class MessageInputStream implements MessageStream
{
	private static final Logger logger = LoggerFactory.getLogger(MessageInputStream.class);
	private Message currentMessage;
	private final CodedInputStream codedInputStream;
	private final InputStream inputStream;
	private int counter = 0;
	private String customerBucketPeriod;
	private ModelIO modelIO = new ModelIO();

	public MessageInputStream(String customerBucketPeriod, InputStream inputStream)
	{
		this.customerBucketPeriod = customerBucketPeriod;
		this.codedInputStream = CodedInputStream.newInstance(inputStream);
		this.inputStream = inputStream;
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

		try
		{
			if(!codedInputStream.isAtEnd())
			{
				byte[] bytes = codedInputStream.readRawBytes(codedInputStream.readRawVarint32());
				currentMessage = modelIO.toMessage(bytes);
				counter++;
			}
			else
			{
				currentMessage = null;
			}

		}
		catch (IOException e)
		{
			throw new RuntimeException("Unable to part messages from InputStream for " + customerBucketPeriod, e);
		}
	}

	public void close()
	{
		try
		{
			logger.debug("Closing Stream (Input) processed {} events for {}", counter, customerBucketPeriod);
			inputStream.close();

		}
		catch (IOException e)
		{
			// nothing we can do about it at this point
		}
	}
}
