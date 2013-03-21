package com.gltech.scale.core.inbound;

import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
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
import com.gltech.scale.util.Props;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.io.EOFException;
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

		// Register the inbound service with the coordination service
		clusterService.getRegistrationService().registerAsInboundService();
	}

	@Override
	public void addMessage(String channelName, MediaType mediaTypes, byte[] payload)
	{
		int maxPayLoadSize = props.get("inbound.max_payload_size_kb", Defaults.MAX_PAYLOAD_SIZE_KB) * 1024;
		Message message = new Message(mediaTypes, payload);
		DateTime nearestPeriodCeiling = timePeriodUtils.nearestPeriodCeiling(message.getReceived_at());
		ChannelMetaData channelMetaData = channelCache.getChannelMetaData(channelName, true);

		// If the payload is too large then write it to the storage engine now
		// and create and message object with no payload, which will flag it as stored.
		if (payload.length > maxPayLoadSize)
		{
			message = new Message(MediaType.APPLICATION_JSON_TYPE);
			storageClient.putMessage(channelMetaData, message.getUuid(), payload);
			logger.debug("Pre-storing message payload data to data store: channelName={} uuid={} bytes={}", channelName, message.getUuid(), payload.length);
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

		if (channelMetaData.isRedundant())
		{
			// This gets a primary and backup aggregator.  Each call with round robin though available sets.
			PrimaryBackupSet primaryBackupSet = aggregatorsByPeriod.nextPrimaryBackupSet();
			aggregatorRestClient.postMessage(primaryBackupSet.getPrimary(), channelName, message);

			if (primaryBackupSet.getBackup() != null)
			{
				aggregatorRestClient.postBackupMessage(primaryBackupSet.getBackup(), channelName, message);
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
			aggregatorRestClient.postMessage(aggregator, channelName, message);
		}
	}

	@Override
	public int writeMessagesToOutputStream(String channelName, DateTime dateTime, OutputStream outputStream, int recordsWritten)
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
		clusterService.getRegistrationService().unRegisterAsInboundService();
	}
}