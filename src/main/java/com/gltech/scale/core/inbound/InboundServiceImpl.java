package com.gltech.scale.core.inbound;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.gltech.scale.core.aggregator.AggregatorsByPeriod;
import com.gltech.scale.core.cluster.ChannelCoordinator;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.core.model.Message;
import com.gltech.scale.core.storage.StorageClient;
import com.google.inject.Inject;
import com.gltech.scale.core.cluster.ClusterService;
import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.core.aggregator.PrimaryBackupSet;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.aggregator.AggregatorRestClient;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.storage.ChannelCache;
import com.gltech.scale.util.Http404Exception;
import com.gltech.scale.util.Props;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InboundServiceImpl implements InboundService
{
	private static final Logger logger = LoggerFactory.getLogger(InboundServiceImpl.class);
	private ConcurrentMap<DateTime, AggregatorsByPeriod> aggregatorPeriodMatrices = new ConcurrentHashMap<>();
	private ClusterService clusterService;
	private ChannelCoordinator channelCoordinator;
	private StorageClient storageClient;
	private AggregatorRestClient aggregatorRestClient;
	private ChannelCache channelCache;
	private TimePeriodUtils timePeriodUtils;
	private Props props = Props.getProps();

	@Inject
	public InboundServiceImpl(ClusterService clusterService, ChannelCoordinator channelCoordinator, StorageClient storageClient, AggregatorRestClient aggregatorRestClient, ChannelCache channelCache, TimePeriodUtils timePeriodUtils)
	{
		this.clusterService = clusterService;
		this.channelCoordinator = channelCoordinator;
		this.storageClient = storageClient;
		this.aggregatorRestClient = aggregatorRestClient;
		this.channelCache = channelCache;
		this.timePeriodUtils = timePeriodUtils;

		// Register the event service with the coordination service
		clusterService.getRegistrationService().registerAsEventService();
	}

	@Override
	public void addEvent(String channelName, MediaType mediaTypes, byte[] payload)
	{
		int maxPayLoadSize = props.get("inbound.max_payload_size_kb", Defaults.MAX_PAYLOAD_SIZE_KB) * 1024;

		Message message = new Message(mediaTypes, payload);
		DateTime nearestPeriodCeiling = timePeriodUtils.nearestPeriodCeiling(message.getReceived_at());

		// If the payload is too large then write it to the storage engine now
		// and create and event object with no payload, which will flag it as stored.
		if (payload.length > maxPayLoadSize)
		{
			message = new Message(MediaType.APPLICATION_JSON_TYPE);
			storageClient.put(channelName, message.getUuid(), payload);
			logger.debug("Pre-storing event payload data to data store: channelName={} uuid={} bytes={}", channelName, message.getUuid(), payload.length);
		}

		AggregatorsByPeriod aggregatorsByPeriod = aggregatorPeriodMatrices.get(nearestPeriodCeiling);

		if (aggregatorsByPeriod == null)
		{
			AggregatorsByPeriod newAggregatorsByPeriod = channelCoordinator.getAggregatorPeriodMatrix(nearestPeriodCeiling);
			aggregatorsByPeriod = aggregatorPeriodMatrices.putIfAbsent(nearestPeriodCeiling, newAggregatorsByPeriod);
			if (aggregatorsByPeriod == null)
			{
				aggregatorsByPeriod = newAggregatorsByPeriod;
			}
		}

		ChannelMetaData channelMetaData = channelCache.getChannelMetaData(channelName, true);

		if (channelMetaData.isRedundant())
		{
			// This gets a primary and backup aggregator.  Each call with round robin though available sets.
			PrimaryBackupSet primaryBackupSet = aggregatorsByPeriod.nextPrimaryBackupSet();
			aggregatorRestClient.postEvent(primaryBackupSet.getPrimary(), message);

			if (primaryBackupSet.getBackup() != null)
			{
				aggregatorRestClient.postBackupEvent(primaryBackupSet.getBackup(), message);
			}
			else
			{
				logger.error("BucketMetaData requires double write redundancy, but no backup aggregators are available.");
			}
		}
		else
		{
			// This gets a aggregator.  Each call with round robin though all (primary and backup) rope managers.
			ServiceMetaData aggregator = aggregatorsByPeriod.next();
			aggregatorRestClient.postEvent(aggregator, message);
		}
	}

	@Override
	public int writeEventsToOutputStream(String channelName, DateTime dateTime, OutputStream outputStream, int recordsWritten)
	{
		String id = timePeriodUtils.nearestPeriodCeiling(dateTime).toString(DateTimeFormat.forPattern("yyyyMMddHHmmss"));

		try
		{
			int origRecordsWritten = recordsWritten;

			// Make sure the stream gets closed.
			try (InputStream inputStream = storageClient.getEventStream(channelName, id))
			{
				JsonFactory f = new MappingJsonFactory();
				JsonParser jp = f.createJsonParser(inputStream);

				if (jp.nextToken() != null)
				{
					while (jp.nextToken() != JsonToken.END_ARRAY)
					{

//						Message message = Message.jsonToEvent(jp);
Message message = null;

						if (recordsWritten > 0)
						{
							outputStream.write(",".getBytes());
						}

						if (message.isStored())
						{
							byte[] payload = storageClient.get(channelName, message.getUuid());
							outputStream.write(payload);
							logger.debug("Reading pre-stored event payload data from storage service: channelName={} uuid={} bytes={}", channelName, message.getUuid(), payload.length);
						}
						else
						{
							outputStream.write(message.getPayload());
						}

						recordsWritten++;
					}
				}

				jp.close();
			}
			catch (IOException e)
			{
				logger.warn("unable to parse json", e);
				throw new RuntimeException("unable to parse json", e);
			}

			logger.debug("Querying events for channelName={} id={} returned {} events.", channelName, id, (recordsWritten - origRecordsWritten));
		}
		catch (Http404Exception e)
		{
			logger.debug("Query returned 404 channelName={} id={} returned {} events.", channelName, id);
		}

		return recordsWritten;
	}

	@Override
	public void shutdown()
	{
		clusterService.getRegistrationService().unRegisterAsEventService();
	}
}