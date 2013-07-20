package com.gltech.scale.core.inbound;

import com.gltech.scale.core.aggregator.AggregatorsByPeriod;
import com.gltech.scale.core.cluster.ChannelCoordinator;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.core.model.Message;
import com.gltech.scale.core.stats.AvgStatOverTime;
import com.gltech.scale.core.stats.StatsManager;
import com.gltech.scale.core.storage.StorageClient;
import com.gltech.scale.core.aggregator.AggregatorClient;
import com.google.inject.Inject;
import com.gltech.scale.core.cluster.ClusterService;
import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.core.aggregator.PrimaryBackupSet;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.storage.ChannelCache;
import com.gltech.scale.util.Props;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;

public class InboundServiceImpl implements InboundService
{
	private static final Logger logger = LoggerFactory.getLogger(InboundServiceImpl.class);
	private ClusterService clusterService;
	private ChannelCoordinator channelCoordinator;
	private StorageClient storageClient;
	private ChannelCache channelCache;
	private AggregatorClient aggregatorClient;
	private TimePeriodUtils timePeriodUtils;
	private AvgStatOverTime addOverSizedMessageTimeStat;
	private Props props = Props.getProps();

	@Inject
	public InboundServiceImpl(ClusterService clusterService, ChannelCoordinator channelCoordinator, StorageClient storageClient, ChannelCache channelCache, TimePeriodUtils timePeriodUtils, StatsManager statsManager, AggregatorClient aggregatorClient)
	{
		this.clusterService = clusterService;
		this.channelCoordinator = channelCoordinator;
		this.storageClient = storageClient;
		this.channelCache = channelCache;
		this.timePeriodUtils = timePeriodUtils;
		this.aggregatorClient = aggregatorClient;

		// Register the inbound service with the coordination service
		clusterService.getRegistrationService().registerAsInboundService();

		String groupName = "Inbound";
		addOverSizedMessageTimeStat = statsManager.createAvgStat(groupName, "AddOverSizedMessage_Time", "milliseconds");
		addOverSizedMessageTimeStat.activateCountStat("AddOverSizedMessage_Count", "message");
	}

	@Override
	public void addMessage(String channelName, MediaType mediaTypes, String queryString, byte[] payload)
	{
		int maxPayLoadSize = props.get("inbound.max_payload_size_kb", Defaults.MAX_PAYLOAD_SIZE_KB) * 1024;
		Message message = new Message(mediaTypes, queryString, payload);
		DateTime nearestPeriodCeiling = timePeriodUtils.nearestPeriodCeiling(message.getReceived_at());
		ChannelMetaData channelMetaData = channelCache.getChannelMetaData(channelName, true);

		// If the payload is too large then write it to the storage engine now
		// and create and message object with no payload, which will flag it as stored.
		if (payload.length > maxPayLoadSize)
		{
			addOverSizedMessageTimeStat.startTimer();
			message = new Message(MediaType.APPLICATION_JSON_TYPE, queryString, new byte[0]);
			storageClient.putMessage(channelMetaData, message.getUuid(), payload);
			addOverSizedMessageTimeStat.stopTimer();
			logger.debug("Pre-storing message payload data to data store: channelName={} uuid={} bytes={}", channelName, message.getUuid(), payload.length);
		}

		// Backed by a cache, so OK to call for every request.
		AggregatorsByPeriod aggregatorsByPeriod = channelCoordinator.getAggregatorPeriodMatrix(nearestPeriodCeiling);

		if (channelMetaData.isRedundant())
		{
			// This gets a primary and backup aggregator.  Each call with round robin though available sets.
			PrimaryBackupSet primaryBackupSet = aggregatorsByPeriod.nextPrimaryBackupSet();
			aggregatorClient.sendMessage(primaryBackupSet.getPrimary(), primaryBackupSet.getBackup(), channelName, nearestPeriodCeiling, message);

			if (primaryBackupSet.getBackup() == null)
			{
				logger.error("BucketMetaData requires double write redundancy, but no backup aggregators are available.");
			}
		}
		else
		{
			// This gets an aggregator.  Each call with round robin though all (primary and backup) rope managers.
			ServiceMetaData aggregator = aggregatorsByPeriod.next();
			aggregatorClient.sendMessage(aggregator, channelName, nearestPeriodCeiling, message);
		}
	}

	@Override
	public void shutdown()
	{
		clusterService.getRegistrationService().unRegisterAsInboundService();
	}
}