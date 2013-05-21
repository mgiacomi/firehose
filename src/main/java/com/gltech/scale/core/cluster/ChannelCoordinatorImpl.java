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

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.*;

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
	private static ScheduledExecutorService scheduledChannelCoordinatorService;
	private static ScheduledExecutorService scheduledPeriodCleanUpService;

	@Inject
	public ChannelCoordinatorImpl(RegistrationService registrationService, TimePeriodUtils timePeriodUtils)
	{
		this.registrationService = registrationService;
		this.timePeriodUtils = timePeriodUtils;

		mapper.registerModule(new JodaModule());
		registeredSetsHist.add(1);
	}

	@Override
	public synchronized void start()
	{
		if (scheduledChannelCoordinatorService == null || scheduledChannelCoordinatorService.isShutdown())
		{
			scheduledChannelCoordinatorService = Executors.newScheduledThreadPool(1, new ThreadFactory()
			{
				public Thread newThread(Runnable runnable)
				{
					return new Thread(runnable, "ChannelCoordinator");
				}
			});

			scheduledChannelCoordinatorService.scheduleAtFixedRate(new Runnable()
			{
				public void run()
				{
					DateTime threeSecondsFromNow = DateTime.now().plusSeconds(3);

					try
					{
						getAggregatorPeriodMatrix(threeSecondsFromNow);
					}
					catch (Exception e)
					{
						// May fail due to network/zookeeper issues.  If so, just try again next time.
						logger.error("Failed to get AggregatorPeriodMatrix for period: " + threeSecondsFromNow, e);
					}
				}
			}, 0, 500, TimeUnit.MILLISECONDS);

			logger.info("ChannelCoordinator has been started.");
		}

		if (scheduledPeriodCleanUpService == null || scheduledPeriodCleanUpService.isShutdown())
		{
			scheduledPeriodCleanUpService = Executors.newScheduledThreadPool(1, new ThreadFactory()
			{
				public Thread newThread(Runnable runnable)
				{
					return new Thread(runnable, "PeriodCleanUpService");
				}
			});

			scheduledPeriodCleanUpService.scheduleAtFixedRate(new Runnable()
			{
				public void run()
				{
					try
					{
						if (client.checkExists().forPath("/aggregator/periods") != null)
						{
							for (String period : client.getChildren().forPath("/aggregator/periods"))
							{
								DateTime periodCeiling = DateTimeFormat.forPattern("yyyyMMddHHmmss").parseDateTime(period);

								if(periodCeiling.isBefore(DateTime.now().minusMinutes(1))) {
									client.delete().forPath("/aggregator/periods/" + period);
								}
							}
						}
					}
					catch (Exception e)
					{
						throw new ClusterException("Failed to clean up aggregators by periods.", e);
					}
				}
			}, 1, 1, TimeUnit.SECONDS);

			logger.info("PeriodCleanUpService has been started.");
		}
	}

	@Override
	public void shutdown()
	{
		scheduledChannelCoordinatorService.shutdown();
		logger.info("ChannelCoordinator has been shutdown.");
	}

	@Override
	public void registerWeight(boolean active, int primaries, int backups, int atRest)
	{
		long weight = new WeightBreakdown(active, primaries, backups, atRest).toWeight();
		ServiceMetaData aggregator = registrationService.getLocalServerMetaData();
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

	@Override
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

	@Override
	public List<AggregatorsByPeriod> getAggregatorsByPeriods()
	{
		List<AggregatorsByPeriod> aggregatorsByPeriods = new ArrayList<>();

		try
		{
			if (client.checkExists().forPath("/aggregator/periods") != null)
			{
				for (String period : client.getChildren().forPath("/aggregator/periods"))
				{
					byte[] data = client.getData().forPath("/aggregator/periods/" + period);
					aggregatorsByPeriods.add(mapper.readValue(new String(data), AggregatorsByPeriod.class));
				}
			}
		}
		catch (Exception e)
		{
			throw new ClusterException("Failed to get aggregators by periods.", e);
		}

		Collections.sort(aggregatorsByPeriods, Collections.reverseOrder());

		return aggregatorsByPeriods;
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

	List<AggregatorsByPeriod> getFuturePeriods(DateTime nearestPeriodCeiling)
	{
		List<AggregatorsByPeriod> aggregatorsByPeriods = new ArrayList<>();

		try
		{
			if (client.checkExists().forPath("/aggregator/periods") != null)
			{
				for (String periodStr : client.getChildren().forPath("/aggregator/periods"))
				{
					DateTime period = DateTimeFormat.forPattern("yyyyMMddHHmmss").parseDateTime(periodStr);

					if(period.isAfter(nearestPeriodCeiling))
					{
						byte[] data = client.getData().forPath("/aggregator/periods/" + periodStr);
						aggregatorsByPeriods.add(mapper.readValue(new String(data), AggregatorsByPeriod.class));
					}
				}
			}
		}
		catch (Exception e)
		{
			throw new ClusterException("Failed to get future aggregators.", e);
		}

		return aggregatorsByPeriods;
	}

	AggregatorsByPeriod writeAggregatorByPeriod(DateTime nearestPeriodCeiling)
	{
		// You can't schedule anything more then one time period into the future.
		int futurePeriodSeconds = 3;

		if (nearestPeriodCeiling.isAfter(timePeriodUtils.nearestPeriodCeiling(DateTime.now()).plusSeconds(futurePeriodSeconds)))
		{
			throw new ClusterException(" You can not schedule aggregator more then " + futurePeriodSeconds + " seconds into the future.");
		}

		try
		{
			if (client.checkExists().forPath("/aggregator/weights") != null)
			{
				List<AggregatorsByPeriod> futurePeriods = getFuturePeriods(nearestPeriodCeiling);
				SortedMap<Long, String> weightsToIds = new TreeMap<>();

				for (String weightToId : client.getChildren().forPath("/aggregator/weights"))
				{
					String[] nodeParts = weightToId.split("\\|");
					long weight = Long.valueOf(nodeParts[0]);
					String workerId = nodeParts[1];

					// Add weights to aggregators assigned to future periods
					for(AggregatorsByPeriod aggregatorsByPeriod : futurePeriods)
					{
						for(PrimaryBackupSet primaryBackupSet : aggregatorsByPeriod.getPrimaryBackupSets())
						{
							if(workerId.equals(primaryBackupSet.getPrimary().getWorkerId().toString()))
							{
								WeightBreakdown weightBreakdown = new WeightBreakdown(weight);
								weightBreakdown.incrementPrimary();
System.out.println("primary: "+ primaryBackupSet.getPrimary().getListenPort() +" : "+ weight +" : "+ weightBreakdown.toWeight());
								weight = weightBreakdown.toWeight();
							}
							if(workerId.equals(primaryBackupSet.getBackup().getWorkerId().toString()))
							{
								WeightBreakdown weightBreakdown = new WeightBreakdown(weight);
								weightBreakdown.incrementBackup();
System.out.println("backup: "+ primaryBackupSet.getBackup().getListenPort() +" : "+ weight +" : "+ weightBreakdown.toWeight());
								weight = weightBreakdown.toWeight();
							}
						}
					}

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

					weightsToIds.put(weight, workerId);
				}

				int totalAggregators = weightsToIds.size();

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

					int index = 0;
					BigDecimal totalAvailableSets = (BigDecimal.valueOf(totalAggregators).divide(BigDecimal.valueOf(2), 0, BigDecimal.ROUND_DOWN));
					BigDecimal percentToAssign = new BigDecimal(Defaults.AGGREGATOR_RESOURCES_PER_PERIOD_PERCENT).divide(BigDecimal.valueOf(100));
					int assignSets = totalAvailableSets.multiply(percentToAssign).setScale(0, BigDecimal.ROUND_DOWN).intValue();

					// We need to have at least one set that we assign.
					if(assignSets < 1) {
						assignSets = 1;
					}

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

					client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/aggregator/periods/" + period, mapper.writeValueAsString(aggregatorsByPeriod).getBytes());
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