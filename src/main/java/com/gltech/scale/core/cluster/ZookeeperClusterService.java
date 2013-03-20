package com.gltech.scale.core.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.gltech.scale.core.cluster.registration.RegistrationService;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.util.ZkClientCreator;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ZookeeperClusterService implements ClusterService
{
	private static final Logger logger = LoggerFactory.getLogger(ZookeeperClusterService.class);
	private CuratorFramework client = ZkClientCreator.createCached();
	private RegistrationService registrationService;

	@Inject
	public ZookeeperClusterService(RegistrationService registrationService)
	{
		this.registrationService = registrationService;
	}

	@Override
	public RegistrationService getRegistrationService()
	{
		return registrationService;
	}

	@Override
	public void registerBatch(ChannelMetaData channelMetaData, DateTime nearestPeriodCeiling)
	{
		try
		{
			BatchPeriodMapper batchPeriodMapper = new BatchPeriodMapper(channelMetaData, nearestPeriodCeiling);

			if (client.checkExists().forPath("/channel/batches/" + batchPeriodMapper.getNodeName()) == null)
			{
				client.create().creatingParentsIfNeeded().forPath("/channel/batches/" + batchPeriodMapper.getNodeName());
				logger.info("Registering batch on channel: " + batchPeriodMapper.getNodeName());
			}
		}
		catch (KeeperException.NodeExistsException nee)
		{
			// It is ok if the node exists.  We only care that it DID get registered.
		}
		catch (Exception e)
		{
			throw new ClusterException("Failed to register aggregator.", e);
		}
	}

	@Override
	public BatchPeriodMapper getOldestCollectibleBatch()
	{
		List<BatchPeriodMapper> activeBatches = getOrderedActiveBucketList();

		try
		{
			ZKPaths.mkdirs(client.getZookeeperClient().getZooKeeper(), "/writer/batches");

			List<BatchPeriodMapper> collecting = getCollectingBucketList();

			for (BatchPeriodMapper activeBatch : activeBatches)
			{
				DateTime collectPoint = activeBatch.getNearestPeriodCeiling().plusSeconds(3);

				if (collectPoint.isBefore(DateTime.now().withMillisOfSecond(0)))
				{
					if (!collecting.contains(activeBatch))
					{
						// Get current Node name and node payload.
						String nodeName = activeBatch.getNodeName();

						// Get node name without path.
						String shortName = BatchPeriodMapper.nodeNameStripPath(nodeName);

						try
						{
							// Convert the ServiceMetaData to a JSON byte[]
							ObjectMapper mapper = new ObjectMapper();
							byte[] collectorManagerMetaData = mapper.writeValueAsBytes(registrationService.getLocalCollectorManagerMetaData());

							// Write to collector time bucket node.
							client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/writer/batches/" + shortName, collectorManagerMetaData);
							logger.info("Registering collector for time bucket: " + nodeName);

							return activeBatch;
						}
						catch (KeeperException.NodeExistsException e)
						{
							// If the node already exists then try the next bucket.
							// I'm putting the continue here even though it is not needed just in case code gets
							// added below this try/catch in the future.  In that case the code will still work
							// as expected.
							continue;
						}
					}
				}
			}
		}
		catch (Exception e)
		{
			throw new ClusterException("Failed to get oldest collectible Batch.", e);
		}

		return null;
	}

	List<BatchPeriodMapper> getCollectingBucketList()
	{
		try
		{
			List<BatchPeriodMapper> mappers = new ArrayList<>();

			if (client.checkExists().forPath("/writer/batches") != null)
			{
				for (String nodeName : client.getChildren().forPath("/writer/batches"))
				{
					mappers.add(new BatchPeriodMapper(nodeName));
				}
			}

			return mappers;
		}
		catch (Exception e)
		{
			throw new ClusterException("Failed to get a list of active buckets.", e);
		}
	}

	List<BatchPeriodMapper> getOrderedActiveBucketList()
	{
		try
		{
			List<BatchPeriodMapper> mappers = new ArrayList<>();

			if (client.checkExists().forPath("/channel/batches") != null)
			{
				for (String nodeName : client.getChildren().forPath("/channel/batches"))
				{
					mappers.add(new BatchPeriodMapper(nodeName));
				}
			}

			Collections.sort(mappers);

			return mappers;
		}
		catch (Exception e)
		{
			throw new ClusterException("Failed to get a list of active buckets.", e);
		}
	}

	@Override
	public void clearCollectorLock(ChannelMetaData channelMetaData, DateTime nearestPeriodCeiling)
	{
		BatchPeriodMapper batchPeriodMapper = new BatchPeriodMapper(channelMetaData, nearestPeriodCeiling);

		try
		{
			client.delete().forPath("/writer/batches/" + batchPeriodMapper.getNodeName());
			logger.info("Clear collector lock for batch: " + batchPeriodMapper.getNodeName());
		}
		catch (Exception e)
		{
			throw new ClusterException("Failed to remove channel and collector for time bucket: " + batchPeriodMapper.getNodeName(), e);
		}
	}

	@Override
	public void clearBatchMetaData(ChannelMetaData channelMetaData, DateTime nearestPeriodCeiling)
	{
		BatchPeriodMapper batchPeriodMapper = new BatchPeriodMapper(channelMetaData, nearestPeriodCeiling);

		try
		{
			client.delete().forPath("/channel/batches/" + batchPeriodMapper.getNodeName());
			logger.info("Removed batch: " + batchPeriodMapper.getNodeName());
		}
		catch (Exception e)
		{
			throw new ClusterException("Failed to remove channel and collector for batch: " + batchPeriodMapper.getNodeName(), e);
		}
	}

	@Override
	public void shutdown()
	{
		registrationService.shutdown();
		logger.info("Zookeeper client is shutdown.");
	}
}
