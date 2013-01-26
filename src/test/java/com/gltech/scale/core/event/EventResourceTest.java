package com.gltech.scale.core.event;

import com.gltech.scale.core.coordination.*;
import com.gltech.scale.core.rope.RopeManagersByPeriod;
import com.gltech.scale.core.coordination.registration.RegistrationService;
import com.gltech.scale.core.coordination.registration.ServiceMetaData;
import com.gltech.scale.core.rope.RopeManagerRestClient;
import com.gltech.scale.core.storage.BucketMetaData;
import com.gltech.scale.core.storage.BucketMetaDataCache;
import com.gltech.scale.core.storage.StorageServiceRestClient;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.ByteArrayOutputStream;

import static org.mockito.Mockito.*;

public class EventResourceTest
{
	EventService eventService;
	StorageServiceRestClient storageServiceRestClient;
	RopeManagerRestClient ropeManagerRestClient;
	BucketMetaDataCache bucketMetaDataCache;
	RopeCoordinator ropeCoordinator;

	@Before
	public void setUp()
	{
		CoordinationService coordinationService = new CoordinationService()
		{
			public DateTime nearestPeriodCeiling(DateTime dateTime)
			{
				return new TimePeriodUtils().nearestPeriodCeiling(dateTime);
			}

			public RopeManagersByPeriod getRopeManagerPeriodMatrix(DateTime dateTime)
			{
				return null;
			}

			public RegistrationService getRegistrationService()
			{
				return mock(RegistrationService.class);
			}

			public BucketPeriodMapper getOldestCollectibleTimeBucket()
			{
				return null;
			}

			public void addTimeBucket(BucketMetaData bucketMetaData, DateTime nearestPeriodCeiling)
			{
			}

			public void clearTimeBucketMetaData(BucketMetaData bucketMetaData, DateTime nearestPeriodCeiling)
			{
			}

			public void clearCollectorLock(BucketMetaData bucketMetaData, DateTime nearestPeriodCeiling)
			{
			}

			public void shutdown()
			{
			}
		};

		storageServiceRestClient = mock(StorageServiceRestClient.class);
		ropeManagerRestClient = mock(RopeManagerRestClient.class);
		bucketMetaDataCache = mock(BucketMetaDataCache.class);
		ropeCoordinator = mock(RopeCoordinator.class);
		eventService = new EventServiceImpl(coordinationService, ropeCoordinator, storageServiceRestClient, ropeManagerRestClient, bucketMetaDataCache, new TimePeriodUtils());
	}

	@Test
	public void testPeriodSecondBySecond() throws Exception
	{
		BucketMetaData bucketMetaData = createBucketMetaData("c1", "b1", 5);
		BucketMetaDataCache bucketMetaDataCache = mock(BucketMetaDataCache.class);
		when(bucketMetaDataCache.getBucketMetaData(anyString(), anyString(), anyBoolean())).thenReturn(bucketMetaData);

		EventResource eventResource = new EventResource(bucketMetaDataCache, eventService);
		Response response = eventResource.getEventsOrRedirect("c1", "b1", 2012, 10, 12, 10, 40, 15);
		((StreamingOutput) response.getEntity()).write(new ByteArrayOutputStream());

		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104015");
	}

	@Test
	public void testPeriodSecondBySecond2() throws Exception
	{
		BucketMetaData bucketMetaData = createBucketMetaData("c1", "b1", 5);
		BucketMetaDataCache bucketMetaDataCache = mock(BucketMetaDataCache.class);
		when(bucketMetaDataCache.getBucketMetaData(anyString(), anyString(), anyBoolean())).thenReturn(bucketMetaData);

		EventResource eventResource = new EventResource(bucketMetaDataCache, eventService);
		Response response = eventResource.getEventsOrRedirect("c1", "b1", 2012, 10, 12, 10, 40, 13);
		((StreamingOutput) response.getEntity()).write(new ByteArrayOutputStream());

		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104015");
	}

	@Test
	public void testPeriodByMinute() throws Exception
	{
		BucketMetaData bucketMetaData = createBucketMetaData("c1", "b1", 5);
		BucketMetaDataCache bucketMetaDataCache = mock(BucketMetaDataCache.class);
		when(bucketMetaDataCache.getBucketMetaData(anyString(), anyString(), anyBoolean())).thenReturn(bucketMetaData);

		EventResource eventResource = new EventResource(bucketMetaDataCache, eventService);
		Response response = eventResource.getEventsOrRedirect("c1", "b1", 2012, 10, 12, 10, 40, -1);
		((StreamingOutput) response.getEntity()).write(new ByteArrayOutputStream());

		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104000");
		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104005");
		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104010");
		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104015");
		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104020");
		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104025");
		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104030");
		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104035");
		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104040");
		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104045");
		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104050");
		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104055");

		verify(storageServiceRestClient, times(12)).getEventStream(any(ServiceMetaData.class), anyString(), anyString(), anyString());
	}

	@Test
	public void testPeriodByHour() throws Exception
	{
		BucketMetaData bucketMetaData = createBucketMetaData("c1", "b1", 5);
		BucketMetaDataCache bucketMetaDataCache = mock(BucketMetaDataCache.class);
		when(bucketMetaDataCache.getBucketMetaData(anyString(), anyString(), anyBoolean())).thenReturn(bucketMetaData);

		EventResource eventResource = new EventResource(bucketMetaDataCache, eventService);
		Response response = eventResource.getEventsOrRedirect("c1", "b1", 2012, 10, 12, 10, -1, -1);
		((StreamingOutput) response.getEntity()).write(new ByteArrayOutputStream());

		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104000");
		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104005");
		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104010");
		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104015");
		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104020");
		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104025");
		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104030");
		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104035");
		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104040");
		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104045");
		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104050");
		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104055");

		verify(storageServiceRestClient, times(720)).getEventStream(any(ServiceMetaData.class), anyString(), anyString(), anyString());
	}

	private BucketMetaData createBucketMetaData(String customer, String bucket, int period)
	{
		return new BucketMetaData(customer, bucket, BucketMetaData.BucketType.eventset, period, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.singlewrite);
	}

}
