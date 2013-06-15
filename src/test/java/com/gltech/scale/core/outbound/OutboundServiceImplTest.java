package com.gltech.scale.core.outbound;

import com.gltech.scale.core.cluster.BatchPeriodMapper;
import com.gltech.scale.core.cluster.ClusterService;
import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.core.cluster.registration.RegistrationService;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.storage.ChannelCache;
import com.gltech.scale.core.storage.StorageClient;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.StreamingOutput;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class OutboundServiceImplTest
{
	OutboundService outboundService;
	ClusterService clusterService;
	StorageClient storageClient;
	ChannelMetaData channelMetaData = new ChannelMetaData("c1", ChannelMetaData.TTL_DAY, false);

	@Before
	public void setUp()
	{
		clusterService = new ClusterService()
		{
			public List<BatchPeriodMapper> getOrderedActiveBucketList()
			{
				return null;
			}

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

		storageClient = mock(StorageClient.class);
		when(storageClient.getMessageStream(any(ChannelMetaData.class), anyString())).thenReturn(new ByteArrayInputStream("".getBytes()));
	}

	@Test
	public void testPeriodSecondBySecond() throws Exception
	{
		ChannelCache channelCache = mock(ChannelCache.class);
		when(channelCache.getChannelMetaData(anyString(), anyBoolean())).thenReturn(channelMetaData);

		outboundService = new OutboundServiceImpl(clusterService, storageClient, channelCache, new TimePeriodUtils());

		StreamingOutput streamingOutput = outboundService.getMessages("c1", 2012, 10, 12, 10, 40, 15);
		streamingOutput.write(new ByteArrayOutputStream());

		verify(storageClient).getMessageStream(channelMetaData, "20121012104015");
		verify(storageClient, times(1)).getMessageStream(any(ChannelMetaData.class), anyString());
	}

	@Test
	public void testPeriodByMinute() throws Exception
	{
		ChannelCache channelCache = mock(ChannelCache.class);
		when(channelCache.getChannelMetaData(anyString(), anyBoolean())).thenReturn(channelMetaData);

		outboundService = new OutboundServiceImpl(clusterService, storageClient, channelCache, new TimePeriodUtils());

		StreamingOutput streamingOutput = outboundService.getMessages("c1", 2012, 10, 12, 10, 40, -1);
		streamingOutput.write(new ByteArrayOutputStream());

		verify(storageClient).getMessageStream(channelMetaData, "20121012104000");
		verify(storageClient).getMessageStream(channelMetaData, "20121012104005");
		verify(storageClient).getMessageStream(channelMetaData, "20121012104010");
		verify(storageClient).getMessageStream(channelMetaData, "20121012104015");
		verify(storageClient).getMessageStream(channelMetaData, "20121012104020");
		verify(storageClient).getMessageStream(channelMetaData, "20121012104025");
		verify(storageClient).getMessageStream(channelMetaData, "20121012104030");
		verify(storageClient).getMessageStream(channelMetaData, "20121012104035");
		verify(storageClient).getMessageStream(channelMetaData, "20121012104040");
		verify(storageClient).getMessageStream(channelMetaData, "20121012104045");
		verify(storageClient).getMessageStream(channelMetaData, "20121012104050");
		verify(storageClient).getMessageStream(channelMetaData, "20121012104055");

		verify(storageClient, times(60)).getMessageStream(any(ChannelMetaData.class), anyString());
	}

	@Test
	public void testPeriodByHour() throws Exception
	{
		ChannelCache channelCache = mock(ChannelCache.class);
		when(channelCache.getChannelMetaData(anyString(), anyBoolean())).thenReturn(channelMetaData);

		outboundService = new OutboundServiceImpl(clusterService, storageClient, channelCache, new TimePeriodUtils());

		StreamingOutput streamingOutput = outboundService.getMessages("c1", 2012, 10, 12, 10, -1, -1);
		streamingOutput.write(new ByteArrayOutputStream());

		verify(storageClient).getMessageStream(channelMetaData, "20121012104000");
		verify(storageClient).getMessageStream(channelMetaData, "20121012104005");
		verify(storageClient).getMessageStream(channelMetaData, "20121012104010");
		verify(storageClient).getMessageStream(channelMetaData, "20121012104015");
		verify(storageClient).getMessageStream(channelMetaData, "20121012104020");
		verify(storageClient).getMessageStream(channelMetaData, "20121012104025");
		verify(storageClient).getMessageStream(channelMetaData, "20121012104030");
		verify(storageClient).getMessageStream(channelMetaData, "20121012104035");
		verify(storageClient).getMessageStream(channelMetaData, "20121012104040");
		verify(storageClient).getMessageStream(channelMetaData, "20121012104045");
		verify(storageClient).getMessageStream(channelMetaData, "20121012104050");
		verify(storageClient).getMessageStream(channelMetaData, "20121012104055");

		verify(storageClient, times(3600)).getMessageStream(any(ChannelMetaData.class), anyString());
	}
}
