package com.gltech.scale.core.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.gltech.scale.core.coordination.registration.RegistrationService;
import com.gltech.scale.core.storage.BucketMetaData;
import com.gltech.scale.core.util.ZkClientCreator;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ZookeeperCoordinationService implements CoordinationService
{
	private static final Logger logger = LoggerFactory.getLogger("com.lokiscale.coordination.ZookeeperCoordinationService");
	private CuratorFramework client = ZkClientCreator.createCached();
	private RegistrationService registrationService;

	@Inject
	public ZookeeperCoordinationService(RegistrationService registrationService)
	{
		this.registrationService = registrationService;
	}

	@Override
	public RegistrationService getRegistrationService()
	{
		return registrationService;
	}

	@Override
	public void addTimeBucket(BucketMetaData bucketMetaData, DateTime nearestPeriodCeiling)
	{
		try
		{
			BucketPeriodMapper bucketPeriodMapper = new BucketPeriodMapper(bucketMetaData, nearestPeriodCeiling);

			if (client.checkExists().forPath("/rope/timebuckets/" + bucketPeriodMapper.getNodeName()) == null)
			{
				client.create().creatingParentsIfNeeded().forPath("/rope/timebuckets/" + bucketPeriodMapper.getNodeName());
				logger.info("Registering time Bucket on rope: " + bucketPeriodMapper.getNodeName());
			}
		}
		catch (KeeperException.NodeExistsException nee)
		{
			// It is ok if the node exists.  We only care that it DID get registered.
		}
		catch (Exception e)
		{
			throw new CoordinationException("Failed to register rope manager.", e);
		}
	}

	@Override
	public BucketPeriodMapper getOldestCollectibleTimeBucket()
	{
		List<BucketPeriodMapper> activeBuckets = getOrderedActiveBucketList();

		try
		{
			ZKPaths.mkdirs(client.getZookeeperClient().getZooKeeper(), "/collector/timebuckets");

			List<BucketPeriodMapper> collecting = getCollectingBucketList();

			for (BucketPeriodMapper activeBucket : activeBuckets)
			{
				DateTime collectPoint = activeBucket.getNearestPeriodCeiling().plusSeconds(3);

				if (collectPoint.isBefore(DateTime.now().withMillisOfSecond(0)))
				{
					if (!collecting.contains(activeBucket))
					{
						// Get current Node name and node payload.
						String nodeName = activeBucket.getNodeName();

						// Get node name without path.
						String shortName = BucketPeriodMapper.nodeNameStripPath(nodeName);

						try
						{
							// Convert the ServiceMetaData to a JSON byte[]
							ObjectMapper mapper = new ObjectMapper();
							byte[] collectorManagerMetaData = mapper.writeValueAsBytes(registrationService.getLocalCollectorManagerMetaData());

							// Write to collector time bucket node.
							client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/collector/timebuckets/" + shortName, collectorManagerMetaData);
							logger.info("Registering collector for time bucket: " + nodeName);

							return activeBucket;
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
			throw new CoordinationException("Failed to get oldest collectible TimeBucketMetaData.", e);
		}

		return null;
	}

	List<BucketPeriodMapper> getCollectingBucketList()
	{
		try
		{
			List<BucketPeriodMapper> mappers = new ArrayList<>();

			if (client.checkExists().forPath("/collector/timebuckets") != null)
			{
				for (String nodeName : client.getChildren().forPath("/collector/timebuckets"))
				{
					mappers.add(new BucketPeriodMapper(nodeName));
				}
			}

			return mappers;
		}
		catch (Exception e)
		{
			throw new CoordinationException("Failed to get a list of active buckets.", e);
		}
	}

	List<BucketPeriodMapper> getOrderedActiveBucketList()
	{
		try
		{
			List<BucketPeriodMapper> mappers = new ArrayList<>();

			if (client.checkExists().forPath("/rope/timebuckets") != null)
			{
				for (String nodeName : client.getChildren().forPath("/rope/timebuckets"))
				{
					mappers.add(new BucketPeriodMapper(nodeName));
				}
			}

			Collections.sort(mappers);

			return mappers;
		}
		catch (Exception e)
		{
			throw new CoordinationException("Failed to get a list of active buckets.", e);
		}
	}

	@Override
	public void clearCollectorLock(BucketMetaData bucketMetaData, DateTime nearestPeriodCeiling)
	{
		BucketPeriodMapper bucketPeriodMapper = new BucketPeriodMapper(bucketMetaData, nearestPeriodCeiling);

		try
		{
			client.delete().forPath("/collector/timebuckets/" + bucketPeriodMapper.getNodeName());
			logger.info("Clear collector lock for time bucket: " + bucketPeriodMapper.getNodeName());
		}
		catch (Exception e)
		{
			throw new CoordinationException("Failed to remove rope and collector for time bucket: " + bucketPeriodMapper.getNodeName(), e);
		}
	}

	@Override
	public void clearTimeBucketMetaData(BucketMetaData bucketMetaData, DateTime nearestPeriodCeiling)
	{
		BucketPeriodMapper bucketPeriodMapper = new BucketPeriodMapper(bucketMetaData, nearestPeriodCeiling);

		try
		{
			client.delete().forPath("/rope/timebuckets/" + bucketPeriodMapper.getNodeName());
			logger.info("Removed TimeBucket: " + bucketPeriodMapper.getNodeName());
		}
		catch (Exception e)
		{
			throw new CoordinationException("Failed to remove rope and collector for time bucket: " + bucketPeriodMapper.getNodeName(), e);
		}
	}

	@Override
	public void shutdown()
	{
		registrationService.shutdown();
		logger.info("Zookeeper client is shutdown.");
	}
}