package com.gltech.scale.core.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.gltech.scale.core.aggregator.AggregatorsByPeriod;
import com.google.inject.Inject;
import com.gltech.scale.core.cluster.registration.RegistrationService;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.aggregator.PrimaryBackupSet;
import com.gltech.scale.util.Props;
import com.gltech.scale.util.ZkClientCreator;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.transaction.CuratorTransaction;
import com.netflix.curator.framework.api.transaction.CuratorTransactionFinal;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class ChannelCoordinatorImpl implements ChannelCoordinator
{
	private static final Logger logger = LoggerFactory.getLogger(ChannelCoordinatorImpl.class);
	private CuratorFramework client = ZkClientCreator.createCached();
	private RegistrationService registrationService;
	private TimePeriodUtils timePeriodUtils;
	private final ObjectMapper mapper = new ObjectMapper();
	private Props props = Props.getProps();
	private List<Integer> registeredSetsHist = new CopyOnWriteArrayList<>();
	private volatile boolean shutdown = false;

	@Inject
	public ChannelCoordinatorImpl(RegistrationService registrationService, TimePeriodUtils timePeriodUtils)
	{
		this.registrationService = registrationService;
		this.timePeriodUtils = timePeriodUtils;

		mapper.registerModule(new JodaModule());
		registeredSetsHist.add(1);
	}

	public void run()
	{
		try
		{
			while (!shutdown)
			{
				DateTime now = DateTime.now().plusSeconds(3);

				try
				{
					getRopeManagerPeriodMatrix(now);
				}
				catch (Exception e)
				{
					// May fail due to network/zookeeper issues.  If so, just try again next time.
					logger.error("Failed to get RopeManagerPeriodMatrix for period: " + now, e);
				}

				Thread.sleep(1000);
			}
		}
		catch (InterruptedException e)
		{
			logger.error("RopeCoordinatorImpl was inturrupted.", e);
		}

		logger.info("RopeCoordinatorImpl has been shutdown.");
	}

	public void shutdown()
	{
		shutdown = true;
	}

	public void registerWeight(boolean active, int primaries, int backups, int restedfor)
	{
		long weight = WeightBreakdown.toWeight(active, primaries, backups, restedfor);
		ServiceMetaData ropeManager = registrationService.getLocalRopeManagerMetaData();

		if (ropeManager != null)
		{
			try
			{
				ZKPaths.mkdirs(client.getZookeeperClient().getZooKeeper(), "/rope_manager/weights");

				String newNode = "/rope_manager/weights/" + weight + "|" + ropeManager.getWorkerId().toString();
				String oldNode = null;

				for (String weightToId : client.getChildren().forPath("/rope_manager/weights"))
				{
					String[] nodeParts = weightToId.split("\\|");
					if (ropeManager.getWorkerId().toString().equals(nodeParts[1]))
					{
						oldNode = "/rope_manager/weights/" + nodeParts[0] + "|" + nodeParts[1];
					}
				}

				if (oldNode != null)
				{
					CuratorTransaction transaction = client.inTransaction();
					CuratorTransactionFinal transactionFinal = transaction.delete().forPath(oldNode).and();
					transactionFinal = transactionFinal.create().withMode(CreateMode.EPHEMERAL).forPath(newNode).and();
					transactionFinal.commit();
				}
				else
				{
					client.create().withMode(CreateMode.EPHEMERAL).forPath(newNode);
				}

				logger.trace("Registering weight={} for rope manager={}:", weight, ropeManager.getWorkerId());
			}
			catch (Exception e)
			{
				throw new ClusterException("Failed to get a list of RopeManager weights.", e);
			}
		}
	}

	public AggregatorsByPeriod getRopeManagerPeriodMatrix(DateTime nearestPeriodCeiling)
	{
		nearestPeriodCeiling = timePeriodUtils.nearestPeriodCeiling(nearestPeriodCeiling);

		AggregatorsByPeriod aggregatorsByPeriod = readRopeManagersByPeriod(nearestPeriodCeiling);

		if (aggregatorsByPeriod == null)
		{
			aggregatorsByPeriod = writeRopeManagerByPeriod(nearestPeriodCeiling);
		}

		return aggregatorsByPeriod;
	}

	AggregatorsByPeriod readRopeManagersByPeriod(DateTime nearestPeriodCeiling)
	{
		String period = nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss"));

		try
		{
			byte[] data = client.getData().forPath("/rope_manager/periods/" + period);
			return mapper.readValue(new String(data), AggregatorsByPeriod.class);
		}
		catch (KeeperException.NoNodeException e)
		{
			return null;
		}
		catch (Exception e)
		{
			throw new ClusterException("Failed to get RopeManager period data for " + period, e);
		}
	}

	AggregatorsByPeriod writeRopeManagerByPeriod(DateTime nearestPeriodCeiling)
	{
		// You can't schedule anything more then one time period into the future.
		int periodSeconds = props.get("coordination.period_seconds", 5);

		if (nearestPeriodCeiling.isAfter(timePeriodUtils.nearestPeriodCeiling(DateTime.now()).plusSeconds(5)))
		{
			throw new ClusterException(" You can not schedule RopeManager more then " + periodSeconds + " seconds into the future.");
		}

		try
		{
			if (client.checkExists().forPath("/rope_manager/weights") != null)
			{
				SortedMap<Long, String> weightsToIds = new TreeMap<>();

				for (String weightToId : client.getChildren().forPath("/rope_manager/weights"))
				{
					String[] nodeParts = weightToId.split("\\|");
					long weight = Long.valueOf(nodeParts[0]);

					while (true)
					{
						if (weightsToIds.containsKey(weight))
						{
							weight++;
						}
						else
						{
							break;
						}
					}

					weightsToIds.put(weight, nodeParts[1]);
				}

				int totalRopeManagers = 0;
				int restingRopeManagers = 0;

				for (long weight : weightsToIds.keySet())
				{
					if (!new WeightBreakdown(weight).isActive())
					{
						restingRopeManagers++;
					}
					totalRopeManagers++;
				}

				List<PrimaryBackupSet> primaryBackupSets = new ArrayList<>();

				// We don't have any rope managers.  Eat shit and die.
				if (totalRopeManagers == 0)
				{
					return null;
				}

				// We only have one total, so just use it.
				else if (totalRopeManagers == 1)
				{
					String id = weightsToIds.get(weightsToIds.firstKey());
					ServiceMetaData primaryRopeManager = registrationService.getRopeManagerMetaDataById(id);
					PrimaryBackupSet primaryBackupSet = new PrimaryBackupSet(primaryRopeManager, null);
					primaryBackupSets.add(primaryBackupSet);
				}

				else if (totalRopeManagers > 1)
				{
					List<String> sortedIds = new ArrayList<>(weightsToIds.values());

					int assignSets = 1;

					if (restingRopeManagers > 3)
					{
						assignSets = restingRopeManagers / 2;

						// Check on past assignments and don't go any higher +1 over recent history
						for (Integer pastSets : registeredSetsHist)
						{
							if (pastSets + 1 < assignSets)
							{
								assignSets = pastSets + 1;
							}
						}
					}

					int index = 0;

					while (assignSets > 0)
					{
						String primaryId = sortedIds.get(index++);
						String secondaryId = sortedIds.get(index++);
						ServiceMetaData primaryRopeManager = registrationService.getRopeManagerMetaDataById(primaryId);
						ServiceMetaData secondaryRopeManager = registrationService.getRopeManagerMetaDataById(secondaryId);
						PrimaryBackupSet primaryBackupSet = new PrimaryBackupSet(primaryRopeManager, secondaryRopeManager);
						primaryBackupSets.add(primaryBackupSet);

						assignSets--;
					}
				}

				try
				{
					String period = nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss"));
					AggregatorsByPeriod aggregatorsByPeriod = new AggregatorsByPeriod(nearestPeriodCeiling, primaryBackupSets);

					client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/rope_manager/periods/" + period, mapper.writeValueAsString(aggregatorsByPeriod).getBytes());
					logger.info("Registering " + primaryBackupSets.size() + " RopeManager set(s) for period: " + period);

					// Add assign set count to the history, but only keep history of the last three
					registeredSetsHist.add(primaryBackupSets.size());
					if (registeredSetsHist.size() > 3)
					{
						registeredSetsHist.remove(0);
					}

					return aggregatorsByPeriod;
				}
				catch (KeeperException.NodeExistsException e)
				{
					// It is fine if it already exists. It just means that someone beat us to it.
					// So let's just return their work.
					return readRopeManagersByPeriod(nearestPeriodCeiling);
				}
			}
		}
		catch (Exception e)
		{
			throw new ClusterException("Failed to write RopeManager by period.", e);
		}

		throw new ClusterException("Failed to write RopeManager by period.");
	}
}

class WeightBreakdown
{
	private boolean active;
	private int primaries;
	private int backups;
	private int restedfor;

	public WeightBreakdown(long weight)
	{
		String weightStr = String.valueOf(weight);

		if (weightStr.substring(0, 1).equals("2"))
		{
			active = true;
		}

		primaries = Integer.valueOf(weightStr.substring(1, 4));
		backups = Integer.valueOf(weightStr.substring(4, 7));
		restedfor = Integer.valueOf(weightStr.substring(7, 10));
	}

	static long toWeight(boolean active, int primaries, int backups, int restedfor)
	{
		StringBuilder weight = new StringBuilder();

		if (active)
		{
			weight.append(2);
		}
		else
		{
			weight.append(1);
		}

		weight.append(String.format("%03d", primaries));
		weight.append(String.format("%03d", backups));
		weight.append(String.format("%03d", restedfor));

		return Long.valueOf(weight.toString());
	}

	public boolean isActive()
	{
		return active;
	}

	public int getPrimaries()
	{
		return primaries;
	}

	public int getBackups()
	{
		return backups;
	}

	public int getRestedfor()
	{
		return restedfor;
	}
}
