package com.gltech.scale.core.inbound;

import com.gltech.scale.core.aggregator.AggregatorsByPeriod;
import com.gltech.scale.core.cluster.*;
import com.gltech.scale.core.cluster.registration.RegistrationService;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.aggregator.AggregatorRestClient;
import com.gltech.scale.core.model.ChannelMetaData;
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

public class InboundResourceTest
{
	InboundService inboundService;
	StorageServiceRestClient storageServiceRestClient;
	AggregatorRestClient aggregatorRestClient;
	BucketMetaDataCache bucketMetaDataCache;
	ChannelCoordinator channelCoordinator;

	@Before
	public void setUp()
	{
		ClusterService clusterService = new ClusterService()
		{
			public DateTime nearestPeriodCeiling(DateTime dateTime)
			{
				return new TimePeriodUtils().nearestPeriodCeiling(dateTime);
			}

			public AggregatorsByPeriod getRopeManagerPeriodMatrix(DateTime dateTime)
			{
				return null;
			}

			public RegistrationService getRegistrationService()
			{
				return mock(RegistrationService.class);
			}

			public BatchPeriodMapper getOldestCollectibleTimeBucket()
			{
				return null;
			}

			public void addTimeBucket(ChannelMetaData bucketMetaData, DateTime nearestPeriodCeiling)
			{
			}

			public void clearTimeBucketMetaData(ChannelMetaData bucketMetaData, DateTime nearestPeriodCeiling)
			{
			}

			public void clearCollectorLock(ChannelMetaData bucketMetaData, DateTime nearestPeriodCeiling)
			{
			}

			public void shutdown()
			{
			}
		};

		storageServiceRestClient = mock(StorageServiceRestClient.class);
		aggregatorRestClient = mock(AggregatorRestClient.class);
		bucketMetaDataCache = mock(BucketMetaDataCache.class);
		channelCoordinator = mock(ChannelCoordinator.class);
		inboundService = new InboundServiceImpl(clusterService, channelCoordinator, storageServiceRestClient, aggregatorRestClient, bucketMetaDataCache, new TimePeriodUtils());
	}

	@Test
	public void testPeriodSecondBySecond() throws Exception
	{
		ChannelMetaData channelMetaData = createBucketMetaData("c1", "b1", 5);
		BucketMetaDataCache bucketMetaDataCache = mock(BucketMetaDataCache.class);
		when(bucketMetaDataCache.getBucketMetaData(anyString(), anyString(), anyBoolean())).thenReturn(channelMetaData);

		InboundResource inboundResource = new InboundResource(bucketMetaDataCache, inboundService);
		Response response = inboundResource.getEventsOrRedirect("c1", "b1", 2012, 10, 12, 10, 40, 15);
		((StreamingOutput) response.getEntity()).write(new ByteArrayOutputStream());

		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104015");
	}

	@Test
	public void testPeriodSecondBySecond2() throws Exception
	{
		ChannelMetaData channelMetaData = createBucketMetaData("c1", "b1", 5);
		BucketMetaDataCache bucketMetaDataCache = mock(BucketMetaDataCache.class);
		when(bucketMetaDataCache.getBucketMetaData(anyString(), anyString(), anyBoolean())).thenReturn(channelMetaData);

		InboundResource inboundResource = new InboundResource(bucketMetaDataCache, inboundService);
		Response response = inboundResource.getEventsOrRedirect("c1", "b1", 2012, 10, 12, 10, 40, 13);
		((StreamingOutput) response.getEntity()).write(new ByteArrayOutputStream());

		verify(storageServiceRestClient).getEventStream(null, "c1", "b1", "20121012104015");
	}

	@Test
	public void testPeriodByMinute() throws Exception
	{
		ChannelMetaData channelMetaData = createBucketMetaData("c1", "b1", 5);
		BucketMetaDataCache bucketMetaDataCache = mock(BucketMetaDataCache.class);
		when(bucketMetaDataCache.getBucketMetaData(anyString(), anyString(), anyBoolean())).thenReturn(channelMetaData);

		InboundResource inboundResource = new InboundResource(bucketMetaDataCache, inboundService);
		Response response = inboundResource.getEventsOrRedirect("c1", "b1", 2012, 10, 12, 10, 40, -1);
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
		ChannelMetaData channelMetaData = createBucketMetaData("c1", "b1", 5);
		BucketMetaDataCache bucketMetaDataCache = mock(BucketMetaDataCache.class);
		when(bucketMetaDataCache.getBucketMetaData(anyString(), anyString(), anyBoolean())).thenReturn(channelMetaData);

		InboundResource inboundResource = new InboundResource(bucketMetaDataCache, inboundService);
		Response response = inboundResource.getEventsOrRedirect("c1", "b1", 2012, 10, 12, 10, -1, -1);
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

	private ChannelMetaData createBucketMetaData(String customer, String bucket, int period)
	{
		return new ChannelMetaData(customer, bucket, ChannelMetaData.BucketType.eventset, period, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
	}

}
