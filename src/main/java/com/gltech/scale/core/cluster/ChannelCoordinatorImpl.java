package com.gltech.scale.core.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.gltech.scale.core.aggregator.AggregatorsByPeriod;
import com.gltech.scale.core.model.Defaults;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public class ChannelCoordinatorImpl implements ChannelCoordinator
{
	private static final Logger logger = LoggerFactory.getLogger(ChannelCoordinatorImpl.class);
	private CuratorFramework client = ZkClientCreator.createCached();
	private RegistrationService registrationService;
	private TimePeriodUtils timePeriodUtils;
	private final ObjectMapper mapper = new ObjectMapper();
	private Props props = Props.getProps();
	private List<Integer> registeredSetsHist = new CopyOnWriteArrayList<>();
	private ConcurrentMap<DateTime, AggregatorsByPeriod> aggregatorPeriodMatricesCache = new ConcurrentHashMap<>();
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
					getAggregatorPeriodMatrix(now);
				}
				catch (Exception e)
				{
					// May fail due to network/zookeeper issues.  If so, just try again next time.
					logger.error("Failed to get AggregatorPeriodMatrix for period: " + now, e);
				}

				TimeUnit.SECONDS.sleep(1);
			}
		}
		catch (InterruptedException e)
		{
			logger.error("ChannelCoordinatorImpl was inturrupted.", e);
		}

		logger.info("ChannelCoordinatorImpl has been shutdown.");
	}

	public void shutdown()
	{
		shutdown = true;
	}

	public void registerWeight(boolean active, int primaries, int backups, int restedfor)
	{
		long weight = WeightBreakdown.toWeight(active, primaries, backups, restedfor);
		ServiceMetaData aggregator = registrationService.getLocalAggregatorMetaData();
		if (aggregator != null)
		{
			try
			{
				ZKPaths.mkdirs(client.getZookeeperClient().getZooKeeper(), "/aggregator/weights");

				String newNode = "/aggregator/weights/" + weight + "|" + aggregator.getWorkerId().toString();
				String oldNode = null;

				for (String weightToId : client.getChildren().forPath("/aggregator/weights"))
				{
					String[] nodeParts = weightToId.split("\\|");
					if (aggregator.getWorkerId().toString().equals(nodeParts[1]))
					{
						oldNode = "/aggregator/weights/" + nodeParts[0] + "|" + nodeParts[1];
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

				logger.trace("Registering weight={} for aggregator={}:", weight, aggregator.getWorkerId());
			}
			catch (Exception e)
			{
				throw new ClusterException("Failed to get a list of aggregator weights.", e);
			}
		}
	}

	public AggregatorsByPeriod getAggregatorPeriodMatrix(DateTime nearestPeriodCeiling)
	{
		nearestPeriodCeiling = timePeriodUtils.nearestPeriodCeiling(nearestPeriodCeiling);

		AggregatorsByPeriod aggregatorsByPeriod = aggregatorPeriodMatricesCache.get(nearestPeriodCeiling);

		// Was it in the cache
		if (aggregatorsByPeriod == null)
		{
			AggregatorsByPeriod newAggregatorsByPeriod = readAggregatorsByPeriod(nearestPeriodCeiling);

			// Was this period already registered with ZK
			if (newAggregatorsByPeriod == null)
			{
				// It has not been registered yet, let's register the period now.
				newAggregatorsByPeriod = writeAggregatorByPeriod(nearestPeriodCeiling);
			}

			// Update the cache
			aggregatorsByPeriod = aggregatorPeriodMatricesCache.putIfAbsent(nearestPeriodCeiling, newAggregatorsByPeriod);
			if (aggregatorsByPeriod == null)
			{
				aggregatorsByPeriod = newAggregatorsByPeriod;
			}
		}

		return aggregatorsByPeriod;
	}

	AggregatorsByPeriod readAggregatorsByPeriod(DateTime nearestPeriodCeiling)
	{
		String period = nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss"));

		try
		{
			byte[] data = client.getData().forPath("/aggregator/periods/" + period);
			return mapper.readValue(new String(data), AggregatorsByPeriod.class);
		}
		catch (KeeperException.NoNodeException e)
		{
			return null;
		}
		catch (Exception e)
		{
			throw new ClusterException("Failed to get aggregator period data for " + period, e);
		}
	}

	AggregatorsByPeriod writeAggregatorByPeriod(DateTime nearestPeriodCeiling)
	{
		// You can't schedule anything more then one time period into the future.
		int periodSeconds = props.get("period_seconds", Defaults.PERIOD_SECONDS);

		if (nearestPeriodCeiling.isAfter(timePeriodUtils.nearestPeriodCeiling(DateTime.now()).plusSeconds(5)))
		{
			throw new ClusterException(" You can not schedule aggregator more then " + periodSeconds + " seconds into the future.");
		}

		try
		{
			if (client.checkExists().forPath("/aggregator/weights") != null)
			{
				SortedMap<Long, String> weightsToIds = new TreeMap<>();

				for (String weightToId : client.getChildren().forPath("/aggregator/weights"))
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

				int totalAggregators = 0;
				int restingAggregators = 0;

				for (long weight : weightsToIds.keySet())
				{
					if (!new WeightBreakdown(weight).isActive())
					{
						restingAggregators++;
					}
					totalAggregators++;
				}

				List<PrimaryBackupSet> primaryBackupSets = new ArrayList<>();

				// We don't have any aggregators.  Eat shit and die.
				if (totalAggregators == 0)
				{
					return null;
				}
				// We only have one total, so just use it.
				else if (totalAggregators == 1)
				{
					String id = weightsToIds.get(weightsToIds.firstKey());
					ServiceMetaData primaryAggregator = registrationService.getAggregatorMetaDataById(id);
					PrimaryBackupSet primaryBackupSet = new PrimaryBackupSet(primaryAggregator, null);
					primaryBackupSets.add(primaryBackupSet);
				}
				else if (totalAggregators > 1)
				{
					List<String> sortedIds = new ArrayList<>(weightsToIds.values());

					int assignSets = 1;

					if (restingAggregators > 3)
					{
						assignSets = restingAggregators / 2;

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
						ServiceMetaData primaryAggregator = registrationService.getAggregatorMetaDataById(primaryId);
						ServiceMetaData secondaryAggregator = registrationService.getAggregatorMetaDataById(secondaryId);
						PrimaryBackupSet primaryBackupSet = new PrimaryBackupSet(primaryAggregator, secondaryAggregator);
						primaryBackupSets.add(primaryBackupSet);

						assignSets--;
					}
				}

				try
				{
					String period = nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss"));
					AggregatorsByPeriod aggregatorsByPeriod = new AggregatorsByPeriod(nearestPeriodCeiling, primaryBackupSets);

					client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/aggregator/periods/" + period, mapper.writeValueAsString(aggregatorsByPeriod).getBytes());
					logger.info("Registering " + primaryBackupSets.size() + " Aggregator set(s) for period: " + period);

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
					return readAggregatorsByPeriod(nearestPeriodCeiling);
				}
			}
		}
		catch (Exception e)
		{
			throw new ClusterException("Failed to write aggregator by period.", e);
		}

		throw new ClusterException("Failed to write aggregator by period.");
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
