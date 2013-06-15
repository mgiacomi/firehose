package com.gltech.scale.core.outbound;

import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import com.gltech.scale.core.cluster.ClusterService;
import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.core.model.Message;
import com.gltech.scale.core.storage.ChannelCache;
import com.gltech.scale.core.storage.StorageClient;
import com.gltech.scale.util.Props;
import com.google.inject.Inject;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class OutboundServiceImpl implements OutboundService
{
	private static final Logger logger = LoggerFactory.getLogger(OutboundServiceImpl.class);
	private ClusterService clusterService;
	private TimePeriodUtils timePeriodUtils;
	private StorageClient storageClient;
	private ChannelCache channelCache;
	private int periodSeconds;
	private Props props = Props.getProps();

	@Inject
	public OutboundServiceImpl(ClusterService clusterService, StorageClient storageClient, ChannelCache channelCache, TimePeriodUtils timePeriodUtils)
	{
		this.clusterService = clusterService;
		this.storageClient = storageClient;
		this.channelCache = channelCache;
		this.timePeriodUtils = timePeriodUtils;

		// Register the inbound service with the coordination service
		clusterService.getRegistrationService().registerAsOutboundService();

		this.periodSeconds = props.get("period_seconds", Defaults.PERIOD_SECONDS);
	}

	@Override
	public StreamingOutput getMessages(final String channelName, final int year, final int month, final int day, final int hour, final int min, final int sec)
	{
		return new StreamingOutput()
		{
			public void write(OutputStream outputStream) throws IOException, WebApplicationException
			{
				outputStream.write("[".getBytes());

				try
				{
					int recordsWritten = 0;

					// If our period is less the 60 and a time with seconds was requested, then make a single request.
					if (sec > -1)
					{
						recordsWritten = writeMessagesToOutputStream(channelName, new DateTime(year, month, day, hour, min, sec), outputStream, recordsWritten);
					}

					// If our period is less the 60 and a time with no secs, but minutes was requested, then query all periods for this minute.
					else if (min > -1)
					{
						for (int i = 0; i < 60; i = i + periodSeconds)
						{
							DateTime dateTime = new DateTime(year, month, day, hour, min, i);

							if (dateTime.isBeforeNow())
							{
								recordsWritten = writeMessagesToOutputStream(channelName, dateTime, outputStream, recordsWritten);
							}
						}
					}

					// If our period is less the 60 and a time with no mins, but hours were requested, then query all periods for this hour.
					else if (hour > -1)
					{
						for (int m = 0; m < 60; m++)
						{
							for (int s = 0; s < 60; s = s + periodSeconds)
							{
								DateTime dateTime = new DateTime(year, month, day, hour, m, s);

								if (dateTime.isBeforeNow())
								{
									recordsWritten = writeMessagesToOutputStream(channelName, dateTime, outputStream, recordsWritten);
								}
							}
						}
					}

					// If our period is less the 60 and a time with no days, but minutes was requested, then query all periods for this day.
					else if (day > -1)
					{
						for (int h = 0; h < 23; h++)
						{
							for (int m = 0; m < 60; m++)
							{
								for (int s = 0; s < 60; s = s + periodSeconds)
								{
									DateTime dateTime = new DateTime(year, month, day, h, m, s);
									if (dateTime.isBeforeNow())
									{
										recordsWritten = writeMessagesToOutputStream(channelName, dateTime, outputStream, recordsWritten);
									}
								}
							}
						}
					}

					logger.debug("Wrote out {} records for channelName={} year={} month={} day={} hour={} min={} sec={}", recordsWritten, channelName, year, month, day, hour, min, sec);
				}
				catch (Exception e)
				{
					throw new WebApplicationException(e);
				}

				outputStream.write("]".getBytes());
			}
		};

	}

	private int writeMessagesToOutputStream(String channelName, DateTime dateTime, OutputStream outputStream, int recordsWritten)
	{
		Schema<Message> schema = RuntimeSchema.getSchema(Message.class);
		String id = timePeriodUtils.nearestPeriodCeiling(dateTime).toString(DateTimeFormat.forPattern("yyyyMMddHHmmss"));
		ChannelMetaData channelMetaData = channelCache.getChannelMetaData(channelName, true);

		int origRecordsWritten = recordsWritten;

		// Make sure the stream gets closed.
		try (InputStream inputStream = storageClient.getMessageStream(channelMetaData, id))
		{
			try
			{
				while (true)
				{
					Message message = new Message();
					ProtostuffIOUtil.mergeDelimitedFrom(inputStream, message, schema);

					if (recordsWritten > 0)
					{
						outputStream.write(",".getBytes());
					}

					if (message.isStored())
					{
						byte[] payload = storageClient.getMessage(channelMetaData, message.getUuid());
						outputStream.write(payload);
						logger.debug("Reading pre-stored message payload data from storage service: channelName={} uuid={} bytes={}", channelName, message.getUuid(), payload.length);
					}
					else
					{
						outputStream.write(message.getPayload());
					}

					recordsWritten++;
				}
			}
			catch (EOFException e)
			{
				// no prob just end of file.
			}
		}
		catch (IOException e)
		{
			logger.warn("unable to parse json", e);
			throw new RuntimeException("unable to parse json", e);
		}

		logger.debug("Querying messages for channelName={} id={} returned {} messages.", channelName, id, (recordsWritten - origRecordsWritten));

		return recordsWritten;
	}

	@Override
	public void shutdown()
	{
		clusterService.getRegistrationService().unRegisterAsOutboundService();
	}
}