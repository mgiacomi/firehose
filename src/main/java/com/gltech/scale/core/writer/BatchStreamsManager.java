package com.gltech.scale.core.writer;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.core.model.Message;
import com.gltech.scale.core.model.ChannelMetaData;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BatchStreamsManager
{
	private static final Logger logger = LoggerFactory.getLogger(BatchStreamsManager.class);
	private List<MessageStream> batchStreams = new ArrayList<>();
	private int totalStreams = 0;
	private Set<String> processedMessages = new HashSet<>();
	private String customerBatchPeriod;
	private long bytesWritten;

	public BatchStreamsManager(ChannelMetaData channelMetaData, DateTime nearestPeriodCeiling)
	{
		customerBatchPeriod = channelMetaData.getName() + "|" + nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")) + "|" + channelMetaData.isRedundant();
	}

	public void registerInputStream(InputStream aggregatorStream)
	{
		batchStreams.add(new MessageInputStream(customerBatchPeriod, aggregatorStream));
		totalStreams++;
	}

	public long writeMessages(OutputStream outputStream)
	{
		logger.debug("Starting aggregator -> storage stream merge. " + customerBatchPeriod);

		long recordsReceived = 0;

		try
		{
			Schema<Message> schema = RuntimeSchema.getSchema(Message.class);
			LinkedBuffer linkedBuffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);

			while (batchStreams.size() > 0)
			{
				MessageStream candidateNextRecord = null;
				for (MessageStream messageStream : new ArrayList<>(batchStreams))
				{
					// If the stream is out of records then remove it from the list and continue loop.
					if (messageStream.getCurrentMessage() == null)
					{
						batchStreams.remove(messageStream);
						messageStream.close();
						continue;
					}

					// If there is no candidate then this is the candidate.
					if (candidateNextRecord == null)
					{
						candidateNextRecord = messageStream;
					}
					else
					{
						// Is this record has already been processed (because of redundancy), then ignore it and advance the stream.
						if (processedMessages.contains(messageStream.getCurrentMessage().getUuid()))
						{
							messageStream.nextRecord();
							recordsReceived++;
						}

						// Is this record the same as the candidateNextRecord (because of redundancy), then ignore it and advance the stream.
						else if (messageStream.getCurrentMessage().equals(candidateNextRecord.getCurrentMessage()))
						{
							messageStream.nextRecord();
							recordsReceived++;
						}

						// If this record older then the current candidateNextRecord then it is the candidate
						else if (messageStream.getCurrentMessage().getReceived_at().isBefore(candidateNextRecord.getCurrentMessage().getReceived_at()))
						{
							candidateNextRecord = messageStream;
						}
					}
				}

				// If we have a candidate then write it to the stream.
				if (candidateNextRecord != null)
				{
					bytesWritten = +ProtostuffIOUtil.writeDelimitedTo(outputStream, candidateNextRecord.getCurrentMessage(), schema, linkedBuffer);
					processedMessages.add(candidateNextRecord.getCurrentMessage().getUuid());
					candidateNextRecord.nextRecord();
					recordsReceived++;
					linkedBuffer.clear();
				}
			}
		}
		catch (IOException e)
		{
			logger.error("BatchStreamManager interrupted " + customerBatchPeriod, e);
		}
		finally
		{
			try
			{
				outputStream.close();
			}
			catch (IOException e)
			{
				logger.error("Error while closing stream "+ customerBatchPeriod, e);
			}

			for (MessageStream messageStream : batchStreams)
			{
				messageStream.close();
			}
		}

		logger.info("Completed stream merge: {}, streams merged={}, processed messages={}, total messages={}, size={}mb", customerBatchPeriod, totalStreams, processedMessages.size(), recordsReceived, bytesWritten / Defaults.MEGABYTES);

		return processedMessages.size();
	}
}
