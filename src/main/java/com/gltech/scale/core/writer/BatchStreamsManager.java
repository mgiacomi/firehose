package com.gltech.scale.core.writer;

import com.gltech.scale.core.model.Message;
import com.gltech.scale.core.model.ChannelMetaData;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BatchStreamsManager
{
	private static final Logger logger = LoggerFactory.getLogger("com.lokiscale.collector.TimeBucketStreamsManager");
	private List<MessageStream> timeBucketStreams = new ArrayList<>();
	private int totalStreams = 0;
	private Set<String> processedEvents = new HashSet<>();
	private String customerBucketPeriod;
	private long bytesWritten;

	public BatchStreamsManager(ChannelMetaData channelMetaData, DateTime nearestPeriodCeiling)
	{
		customerBucketPeriod = channelMetaData.getCustomer() + "|" + channelMetaData.getBucket() + "|" + nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")) + "|" + channelMetaData.getRedundancy();
	}

	public void registerInputStream(InputStream ropeStream)
	{
		timeBucketStreams.add(new MessageInputStream(customerBucketPeriod, ropeStream));
		totalStreams++;
	}

	public void registerEventList(List<Message> events)
	{
		timeBucketStreams.add(new MessageListStream(customerBucketPeriod, events));
		totalStreams++;
	}

	public long writeEvents(OutputStream outputStream)
	{
		logger.debug("Starting rope -> storage stream merge. " + customerBucketPeriod);

		long recordsReceived = 0;

		try
		{
			byte[] start = "[".getBytes();
			outputStream.write(start);
			bytesWritten = bytesWritten + start.length;

			while (timeBucketStreams.size() > 0)
			{
				MessageStream candidateNextRecord = null;
				for (MessageStream messageStream : new ArrayList<>(timeBucketStreams))
				{
					// If the stream is out of records then remove it from the list and continue loop.
					if (messageStream.getCurrentMessage() == null)
					{
						timeBucketStreams.remove(messageStream);
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
						// Is this record has already been processed (because of doublewrite), then ignore it and advance the stream.
						if (processedEvents.contains(messageStream.getCurrentMessage().getUuid()))
						{
							messageStream.nextRecord();
							recordsReceived++;
						}

						// Is this record the same as the candidateNextRecord (because of doublewrite), then ignore it and advance the stream.
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
					if (processedEvents.size() > 0)
					{
						byte[] comma = ",".getBytes();
						outputStream.write(comma);
						bytesWritten = bytesWritten + comma.length;

					}

					byte[] data = candidateNextRecord.getCurrentMessage().toJson().toString().getBytes();
					outputStream.write(data);
					bytesWritten = bytesWritten + data.length;
					processedEvents.add(candidateNextRecord.getCurrentMessage().getUuid());
					candidateNextRecord.nextRecord();
					recordsReceived++;
				}
			}

			byte[] end = "]".getBytes();
			outputStream.write(end);
			bytesWritten = bytesWritten + end.length;
		}
		catch (IOException e)
		{
			logger.error("TimeBucketStreamManager interrupted " + customerBucketPeriod, e);
		}
		finally
		{
			try
			{
				outputStream.close();
			}
			catch (IOException e)
			{
				// Ignore
			}

			for (MessageStream messageStream : timeBucketStreams)
			{
				messageStream.close();
			}
		}

		logger.info("Merged " + totalStreams + " streams, " + processedEvents.size() + " of " + recordsReceived + " events " + customerBucketPeriod + " totaling " + bytesWritten + "mb.");

		return processedEvents.size();
	}
}
