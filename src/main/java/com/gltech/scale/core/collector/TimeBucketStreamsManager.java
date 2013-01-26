package com.gltech.scale.core.collector;

import com.gltech.scale.core.event.EventPayload;
import com.gltech.scale.core.storage.BucketMetaData;
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

public class TimeBucketStreamsManager
{
	private static final Logger logger = LoggerFactory.getLogger("com.lokiscale.collector.TimeBucketStreamsManager");
	private List<EventStream> timeBucketStreams = new ArrayList<>();
	private int totalStreams = 0;
	private Set<String> processedEvents = new HashSet<>();
	private String customerBucketPeriod;
	private long bytesWritten;

	public TimeBucketStreamsManager(BucketMetaData bucketMetaData, DateTime nearestPeriodCeiling)
	{
		customerBucketPeriod = bucketMetaData.getCustomer() + "|" + bucketMetaData.getBucket() + "|" + nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")) + "|" + bucketMetaData.getRedundancy();
	}

	public void registerInputStream(InputStream ropeStream)
	{
		timeBucketStreams.add(new EventInputStream(customerBucketPeriod, ropeStream));
		totalStreams++;
	}

	public void registerEventList(List<EventPayload> events)
	{
		timeBucketStreams.add(new EventListStream(customerBucketPeriod, events));
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
				EventStream candidateNextRecord = null;
				for (EventStream eventStream : new ArrayList<>(timeBucketStreams))
				{
					// If the stream is out of records then remove it from the list and continue loop.
					if (eventStream.getCurrentEventPayload() == null)
					{
						timeBucketStreams.remove(eventStream);
						eventStream.close();
						continue;
					}

					// If there is no candidate then this is the candidate.
					if (candidateNextRecord == null)
					{
						candidateNextRecord = eventStream;
					}
					else
					{
						// Is this record has already been processed (because of doublewrite), then ignore it and advance the stream.
						if (processedEvents.contains(eventStream.getCurrentEventPayload().getUuid()))
						{
							eventStream.nextRecord();
							recordsReceived++;
						}

						// Is this record the same as the candidateNextRecord (because of doublewrite), then ignore it and advance the stream.
						else if (eventStream.getCurrentEventPayload().equals(candidateNextRecord.getCurrentEventPayload()))
						{
							eventStream.nextRecord();
							recordsReceived++;
						}

						// If this record older then the current candidateNextRecord then it is the candidate
						else if (eventStream.getCurrentEventPayload().getReceived_at().isBefore(candidateNextRecord.getCurrentEventPayload().getReceived_at()))
						{
							candidateNextRecord = eventStream;
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

					byte[] data = candidateNextRecord.getCurrentEventPayload().toJson().toString().getBytes();
					outputStream.write(data);
					bytesWritten = bytesWritten + data.length;
					processedEvents.add(candidateNextRecord.getCurrentEventPayload().getUuid());
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

			for (EventStream eventStream : timeBucketStreams)
			{
				eventStream.close();
			}
		}

		logger.info("Merged " + totalStreams + " streams, " + processedEvents.size() + " of " + recordsReceived + " events " + customerBucketPeriod + " totaling " + bytesWritten + "mb.");

		return processedEvents.size();
	}
}
