package com.gltech.scale.core.inbound;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.gltech.scale.core.model.Message;
import com.google.inject.Inject;
import com.gltech.scale.core.coordination.CoordinationService;
import com.gltech.scale.core.coordination.RopeCoordinator;
import com.gltech.scale.core.coordination.TimePeriodUtils;
import com.gltech.scale.core.rope.PrimaryBackupSet;
import com.gltech.scale.core.rope.RopeManagersByPeriod;
import com.gltech.scale.core.coordination.registration.ServiceMetaData;
import com.gltech.scale.core.rope.RopeManagerRestClient;
import com.gltech.scale.core.storage.BucketMetaData;
import com.gltech.scale.core.storage.BucketMetaDataCache;
import com.gltech.scale.core.storage.StorageServiceClient;
import com.gltech.scale.core.util.Http404Exception;
import com.gltech.scale.core.util.Props;
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
	private ConcurrentMap<DateTime, RopeManagersByPeriod> ropeManagerPeriodMatrices = new ConcurrentHashMap<>();
	private CoordinationService coordinationService;
	private RopeCoordinator ropeCoordinator;
	private StorageServiceClient storageServiceClient;
	private RopeManagerRestClient ropeManagerRestClient;
	private BucketMetaDataCache bucketMetaDataCache;
	private TimePeriodUtils timePeriodUtils;
	private Props props = Props.getProps();

	@Inject
	public InboundServiceImpl(CoordinationService coordinationService, RopeCoordinator ropeCoordinator, StorageServiceClient storageServiceClient, RopeManagerRestClient ropeManagerRestClient, BucketMetaDataCache bucketMetaDataCache, TimePeriodUtils timePeriodUtils)
	{
		this.coordinationService = coordinationService;
		this.ropeCoordinator = ropeCoordinator;
		this.storageServiceClient = storageServiceClient;
		this.ropeManagerRestClient = ropeManagerRestClient;
		this.bucketMetaDataCache = bucketMetaDataCache;
		this.timePeriodUtils = timePeriodUtils;

		// Register the event service with the coordination service
		coordinationService.getRegistrationService().registerAsEventService();
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
			ServiceMetaData storageService = coordinationService.getRegistrationService().getStorageServiceRoundRobin();
			storageServiceClient.put(storageService, customer, bucket, message.getUuid(), payload);
			logger.debug("Pre-storing event payload data to storage service: customer={} bucket={} uuid={} bytes={}", message.getCustomer(), message.getBucket(), message.getUuid(), payload.length);
		}

		RopeManagersByPeriod ropeManagersByPeriod = ropeManagerPeriodMatrices.get(nearestPeriodCeiling);

		if (ropeManagersByPeriod == null)
		{
			RopeManagersByPeriod newRopeManagersByPeriod = ropeCoordinator.getRopeManagerPeriodMatrix(nearestPeriodCeiling);
			ropeManagersByPeriod = ropeManagerPeriodMatrices.putIfAbsent(nearestPeriodCeiling, newRopeManagersByPeriod);
			if (ropeManagersByPeriod == null)
			{
				ropeManagersByPeriod = newRopeManagersByPeriod;
			}
		}

		BucketMetaData bucketMetaData = bucketMetaDataCache.getBucketMetaData(message.getCustomer(), message.getBucket(), true);

		if (bucketMetaData.getRedundancy().equals(BucketMetaData.Redundancy.singlewrite))
		{
			// This gets a rope manager.  Each call with round robin though all (primary and backup) rope managers.
			ServiceMetaData ropeManager = ropeManagersByPeriod.next();
			ropeManagerRestClient.postEvent(ropeManager, message);
		}

		if (bucketMetaData.getRedundancy().equals(BucketMetaData.Redundancy.doublewritesync))
		{
			// This gets a primary and backup rope manager.  Each call with round robin though available sets.
			PrimaryBackupSet primaryBackupSet = ropeManagersByPeriod.nextPrimaryBackupSet();
			ropeManagerRestClient.postEvent(primaryBackupSet.getPrimary(), message);

			if (primaryBackupSet.getBackup() != null)
			{
				ropeManagerRestClient.postBackupEvent(primaryBackupSet.getBackup(), message);
			}
			else
			{
				logger.error("BucketMetaData requires double write redundancy, but no backup rope managers are available.");
			}
		}
	}

	@Override
	public int writeEventsToOutputStream(BucketMetaData bucketMetaData, DateTime dateTime, OutputStream outputStream, int recordsWritten)
	{
		String id = timePeriodUtils.nearestPeriodCeiling(dateTime).toString(DateTimeFormat.forPattern("yyyyMMddHHmmss"));
		ServiceMetaData storageService = coordinationService.getRegistrationService().getStorageServiceRoundRobin();

		String customer = bucketMetaData.getCustomer();
		String bucket = bucketMetaData.getBucket();

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
		coordinationService.getRegistrationService().unRegisterAsEventService();
	}
}