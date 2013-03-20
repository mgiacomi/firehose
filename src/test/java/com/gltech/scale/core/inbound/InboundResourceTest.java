package com.gltech.scale.core.inbound;

import com.gltech.scale.core.cluster.*;
import com.gltech.scale.core.cluster.registration.RegistrationService;
import com.gltech.scale.core.aggregator.AggregatorRestClient;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.storage.ChannelCache;
import com.gltech.scale.core.storage.StorageClient;
import com.gltech.scale.util.ModelIO;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.mockito.Mockito.*;

public class InboundResourceTest
{
	InboundService inboundService;
	AggregatorRestClient aggregatorRestClient;
	ChannelCache channelCache;
	StorageClient storageClient;
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

			public RegistrationService getRegistrationService()
			{
				return mock(RegistrationService.class);
			}

			public BatchPeriodMapper getOldestCollectibleBatch()
			{
				return null;
			}

			public void registerBatch(ChannelMetaData bucketMetaData, DateTime nearestPeriodCeiling)
			{
			}

			public void clearBatchMetaData(ChannelMetaData bucketMetaData, DateTime nearestPeriodCeiling)
			{
			}

			public void clearStorageWriterLock(ChannelMetaData channelMetaData, DateTime nearestPeriodCeiling)
			{
			}

			public void shutdown()
			{
			}
		};

		aggregatorRestClient = mock(AggregatorRestClient.class);
		channelCache = mock(ChannelCache.class);
		storageClient = mock(StorageClient.class);
		channelCoordinator = mock(ChannelCoordinator.class);
		inboundService = new InboundServiceImpl(clusterService, channelCoordinator, storageClient, aggregatorRestClient, channelCache, new TimePeriodUtils());

		when(storageClient.getMessageStream(anyString(), anyString())).thenReturn(new ByteArrayInputStream("".getBytes()));
	}

	@Test
	public void testPeriodSecondBySecond() throws Exception
	{
		ChannelMetaData channelMetaData = createBucketMetaData("c1", 5);
		ChannelCache channelCache = mock(ChannelCache.class);
		when(channelCache.getChannelMetaData(anyString(), anyBoolean())).thenReturn(channelMetaData);

		InboundResource inboundResource = new InboundResource(channelCache, inboundService, new ModelIO(), storageClient);
		Response response = inboundResource.getMessagesOrRedirect("c1", 2012, 10, 12, 10, 40, 15);
		((StreamingOutput) response.getEntity()).write(new ByteArrayOutputStream());

		verify(storageClient).getMessageStream("c1", "20121012104015");
	}

	@Test
	public void testPeriodSecondBySecond2() throws Exception
	{
		ChannelMetaData channelMetaData = createBucketMetaData("c1", 5);
		ChannelCache channelCache = mock(ChannelCache.class);
		when(channelCache.getChannelMetaData(anyString(), anyBoolean())).thenReturn(channelMetaData);

		InboundResource inboundResource = new InboundResource(channelCache, inboundService, new ModelIO(), storageClient);
		Response response = inboundResource.getMessagesOrRedirect("c1", 2012, 10, 12, 10, 40, 13);
		((StreamingOutput) response.getEntity()).write(new ByteArrayOutputStream());

		verify(storageClient).getMessageStream("c1", "20121012104015");
	}

	@Test
	public void testPeriodByMinute() throws Exception
	{
		ChannelMetaData channelMetaData = createBucketMetaData("c1", 5);
		ChannelCache channelCache = mock(ChannelCache.class);
		when(channelCache.getChannelMetaData(anyString(), anyBoolean())).thenReturn(channelMetaData);

		InboundResource inboundResource = new InboundResource(channelCache, inboundService, new ModelIO(), storageClient);
		Response response = inboundResource.getMessagesOrRedirect("c1", 2012, 10, 12, 10, 40, -1);
		((StreamingOutput) response.getEntity()).write(new ByteArrayOutputStream());

		verify(storageClient).getMessageStream("c1", "20121012104000");
		verify(storageClient).getMessageStream("c1", "20121012104005");
		verify(storageClient).getMessageStream("c1", "20121012104010");
		verify(storageClient).getMessageStream("c1", "20121012104015");
		verify(storageClient).getMessageStream("c1", "20121012104020");
		verify(storageClient).getMessageStream("c1", "20121012104025");
		verify(storageClient).getMessageStream("c1", "20121012104030");
		verify(storageClient).getMessageStream("c1", "20121012104035");
		verify(storageClient).getMessageStream("c1", "20121012104040");
		verify(storageClient).getMessageStream("c1", "20121012104045");
		verify(storageClient).getMessageStream("c1", "20121012104050");
		verify(storageClient).getMessageStream("c1", "20121012104055");

		verify(storageClient, times(12)).getMessageStream(anyString(), anyString());
	}

	@Test
	public void testPeriodByHour() throws Exception
	{
		ChannelMetaData channelMetaData = createBucketMetaData("c1", 5);
		ChannelCache channelCache = mock(ChannelCache.class);
		when(channelCache.getChannelMetaData(anyString(), anyBoolean())).thenReturn(channelMetaData);

		InboundResource inboundResource = new InboundResource(channelCache, inboundService, new ModelIO(), storageClient);
		Response response = inboundResource.getMessagesOrRedirect("c1", 2012, 10, 12, 10, -1, -1);
		((StreamingOutput) response.getEntity()).write(new ByteArrayOutputStream());

		verify(storageClient).getMessageStream("c1", "20121012104000");
		verify(storageClient).getMessageStream("c1", "20121012104005");
		verify(storageClient).getMessageStream("c1", "20121012104010");
		verify(storageClient).getMessageStream("c1", "20121012104015");
		verify(storageClient).getMessageStream("c1", "20121012104020");
		verify(storageClient).getMessageStream("c1", "20121012104025");
		verify(storageClient).getMessageStream("c1", "20121012104030");
		verify(storageClient).getMessageStream("c1", "20121012104035");
		verify(storageClient).getMessageStream("c1", "20121012104040");
		verify(storageClient).getMessageStream("c1", "20121012104045");
		verify(storageClient).getMessageStream("c1", "20121012104050");
		verify(storageClient).getMessageStream("c1", "20121012104055");

		verify(storageClient, times(720)).getMessageStream(anyString(), anyString());
	}

	private ChannelMetaData createBucketMetaData(String channelName, int period)
	{
		return new ChannelMetaData(channelName, ChannelMetaData.TTL_DAY, false);
	}
}
