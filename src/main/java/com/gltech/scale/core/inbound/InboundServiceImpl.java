package com.gltech.scale.core.inbound;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.gltech.scale.core.aggregator.AggregatorsByPeriod;
import com.gltech.scale.core.cluster.ChannelCoordinator;
import com.gltech.scale.core.model.Message;
import com.google.inject.Inject;
import com.gltech.scale.core.cluster.ClusterService;
import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.core.aggregator.PrimaryBackupSet;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.aggregator.AggregatorRestClient;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.storage.BucketMetaDataCache;
import com.gltech.scale.core.storage.StorageServiceClient;
import com.gltech.scale.util.Http404Exception;
import com.gltech.scale.util.Props;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InboundServiceImpl implements InboundService
{
	private static final Logger logger = LoggerFactory.getLogger("com.lokiscale.event.EventServiceImpl");
	private ConcurrentMap<DateTime, AggregatorsByPeriod> ropeManagerPeriodMatrices = new ConcurrentHashMap<>();
	private ClusterService clusterService;
	private ChannelCoordinator channelCoordinator;
	private StorageServiceClient storageServiceClient;
	private AggregatorRestClient aggregatorRestClient;
	private BucketMetaDataCache bucketMetaDataCache;
	private TimePeriodUtils timePeriodUtils;
	private Props props = Props.getProps();

	@Inject
	public InboundServiceImpl(ClusterService clusterService, ChannelCoordinator channelCoordinator, StorageServiceClient storageServiceClient, AggregatorRestClient aggregatorRestClient, BucketMetaDataCache bucketMetaDataCache, TimePeriodUtils timePeriodUtils)
	{
		this.clusterService = clusterService;
		this.channelCoordinator = channelCoordinator;
		this.storageServiceClient = storageServiceClient;
		this.aggregatorRestClient = aggregatorRestClient;
		this.bucketMetaDataCache = bucketMetaDataCache;
		this.timePeriodUtils = timePeriodUtils;

		// Register the event service with the coordination service
		clusterService.getRegistrationService().registerAsEventService();
	}

	@Override
	public void addEvent(String customer, String bucket, byte[] payload)
	{
		int maxPayLoadSize = props.get("event_service.max_payload_size_kb", 50) * 1024;

		Message message = new Message(customer, bucket, payload);
		DateTime nearestPeriodCeiling = timePeriodUtils.nearestPeriodCeiling(message.getReceived_at());

		// If the payload is too large then write it to the storage engine now
		// and create and event object with no payload, which will flag it as stored.
		if (payload.length > maxPayLoadSize)
		{
			message = new Message(customer, bucket);
			ServiceMetaData storageService = clusterService.getRegistrationService().getStorageServiceRoundRobin();
			storageServiceClient.put(storageService, customer, bucket, message.getUuid(), payload);
			logger.debug("Pre-storing event payload data to storage service: customer={} bucket={} uuid={} bytes={}", message.getCustomer(), message.getBucket(), message.getUuid(), payload.length);
		}

		AggregatorsByPeriod aggregatorsByPeriod = ropeManagerPeriodMatrices.get(nearestPeriodCeiling);

		if (aggregatorsByPeriod == null)
		{
			AggregatorsByPeriod newAggregatorsByPeriod = channelCoordinator.getRopeManagerPeriodMatrix(nearestPeriodCeiling);
			aggregatorsByPeriod = ropeManagerPeriodMatrices.putIfAbsent(nearestPeriodCeiling, newAggregatorsByPeriod);
			if (aggregatorsByPeriod == null)
			{
				aggregatorsByPeriod = newAggregatorsByPeriod;
			}
		}

		ChannelMetaData channelMetaData = bucketMetaDataCache.getBucketMetaData(message.getCustomer(), message.getBucket(), true);

		if (channelMetaData.getRedundancy().equals(ChannelMetaData.Redundancy.singlewrite))
		{
			// This gets a rope manager.  Each call with round robin though all (primary and backup) rope managers.
			ServiceMetaData ropeManager = aggregatorsByPeriod.next();
			aggregatorRestClient.postEvent(ropeManager, message);
		}

		if (channelMetaData.getRedundancy().equals(ChannelMetaData.Redundancy.doublewritesync))
		{
			// This gets a primary and backup rope manager.  Each call with round robin though available sets.
			PrimaryBackupSet primaryBackupSet = aggregatorsByPeriod.nextPrimaryBackupSet();
			aggregatorRestClient.postEvent(primaryBackupSet.getPrimary(), message);

			if (primaryBackupSet.getBackup() != null)
			{
				aggregatorRestClient.postBackupEvent(primaryBackupSet.getBackup(), message);
			}
			else
			{
				logger.error("BucketMetaData requires double write redundancy, but no backup rope managers are available.");
			}
		}
	}

	@Override
	public int writeEventsToOutputStream(ChannelMetaData channelMetaData, DateTime dateTime, OutputStream outputStream, int recordsWritten)
	{
		String id = timePeriodUtils.nearestPeriodCeiling(dateTime).toString(DateTimeFormat.forPattern("yyyyMMddHHmmss"));
		ServiceMetaData storageService = clusterService.getRegistrationService().getStorageServiceRoundRobin();

		String customer = channelMetaData.getCustomer();
		String bucket = channelMetaData.getBucket();

		try
		{
			int origRecordsWritten = recordsWritten;

			// Make sure the stream gets closed.
			try (InputStream inputStream = storageServiceClient.getEventStream(storageService, customer, bucket, id))
			{
				JsonFactory f = new MappingJsonFactory();
				JsonParser jp = f.createJsonParser(inputStream);

				if (jp.nextToken() != null)
				{
					while (jp.nextToken() != JsonToken.END_ARRAY)
					{

						Message message = Message.jsonToEvent(jp);

						if (recordsWritten > 0)
						{
							outputStream.write(",".getBytes());
						}

						if (message.isStored())
						{
							byte[] payload = storageServiceClient.get(storageService, customer, bucket, message.getUuid());
							outputStream.write(payload);
							logger.debug("Reading pre-stored event payload data from storage service: customer={} bucket={} uuid={} bytes={}", message.getCustomer(), message.getBucket(), message.getUuid(), payload.length);
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

			logger.debug("Querying events for customer={} bucket={} id={} returned {} events.", customer, bucket, id, (recordsWritten - origRecordsWritten));
		}
		catch (Http404Exception e)
		{
			logger.debug("Query returned 404 customer={} bucket={} id={} returned {} events.", customer, bucket, id);
		}

		return recordsWritten;
	}

	@Override
	public void shutdown()
	{
		clusterService.getRegistrationService().unRegisterAsEventService();
	}
}