package com.gltech.scale.monitoring.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.gltech.scale.core.aggregator.AggregatorsByPeriod;
import com.gltech.scale.core.cluster.BatchPeriodMapper;
import com.gltech.scale.core.cluster.ChannelCoordinator;
import com.gltech.scale.core.cluster.ClusterService;
import com.gltech.scale.core.model.BatchMetaData;
import com.gltech.scale.core.stats.results.AvgStat;
import com.gltech.scale.core.stats.results.GroupStats;
import com.gltech.scale.core.stats.results.OverTime;
import com.gltech.scale.monitoring.model.*;
import com.google.inject.Inject;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;

public class ClusterStatsServiceImpl implements ClusterStatsService
{
	private static final Logger logger = LoggerFactory.getLogger(ClusterStatsServiceImpl.class);
	private ConcurrentMap<String, ServerStats> serverStatsMap = new ConcurrentHashMap<>();
	private Set<ClusterStatsCallBack> callBacks = new CopyOnWriteArraySet<>();
	private ClusterService clusterService;
	private ChannelCoordinator channelCoordinator;
	private ResultsIO resultsIO;
	private final ObjectMapper mapper = new ObjectMapper();

	@Inject
	public ClusterStatsServiceImpl(ClusterService clusterService, ChannelCoordinator channelCoordinator, ResultsIO resultsIO)
	{
		this.clusterService = clusterService;
		this.channelCoordinator = channelCoordinator;
		this.resultsIO = resultsIO;

		mapper.registerModule(new JodaModule());
	}

	@Override
	public void updateGroupStats(List<ServerStats> serverStatsList)
	{
		ConcurrentMap<String, ServerStats> newServerStatsMap = new ConcurrentHashMap<>();
		for (ServerStats serverStats : serverStatsList)
		{
			newServerStatsMap.put(serverStats.getWorkerId(), serverStats);
		}

		serverStatsMap = newServerStatsMap;

		for (ClusterStatsCallBack clusterStatsCallBack : callBacks)
		{
			clusterStatsCallBack.clusterStatsUpdate(getClusterStats(serverStatsList));
		}
	}

	@Override
	public String getJsonStatsAll()
	{
		List<ServerStats> sortedStatsList = new ArrayList<>(serverStatsMap.values());
		Collections.sort(sortedStatsList, new Comparator<ServerStats>()
		{
			public int compare(ServerStats o1, ServerStats o2)
			{
				return o1.getWorkerId().compareTo(o2.getWorkerId());
			}
		});

		return resultsIO.toJson(getClusterStats(sortedStatsList));
	}

	@Override
	public void registerCallback(ClusterStatsCallBack clusterStatsCallBack)
	{
		callBacks.add(clusterStatsCallBack);
	}

	@Override
	public void unRegisterCallback(ClusterStatsCallBack clusterStatsCallBack)
	{
		callBacks.remove(clusterStatsCallBack);
	}

	private ClusterStats getClusterStats(List<ServerStats> serverStatsList)
	{
		ClusterStats clusterStats = new ClusterStats();
		clusterStats.setStats(serverStatsList);

		for (ServerStats serverStats : serverStatsList)
		{
			// Aggregrate stats by group and name
			for (GroupStats groupStats : serverStats.getGroupStatsList().values())
			{
				// If the group doesn't exist create it
				if (!clusterStats.getAggregateStats().containsKey(groupStats.getName()))
				{
					clusterStats.getAggregateStats().put(groupStats.getName(), new HashMap<String, AggregateOverTime>());
				}

				Map<String, AggregateOverTime> aggregateOverTimeMap = clusterStats.getAggregateStats().get(groupStats.getName());

				genAvgCountHighLow(groupStats, aggregateOverTimeMap);
			}

			GroupStats commonGroup = null;

			for (GroupStats groupStats : serverStats.getGroupStatsList().values())
			{
				if ("common".equalsIgnoreCase(groupStats.getName()))
				{
					commonGroup = groupStats;
				}
			}

			// If the role group doesn't exist create it
			if (commonGroup != null)
			{
				for (String role : serverStats.getRoles())
				{
					String roleName = "role_" + role;
					if (!clusterStats.getAggregateStats().containsKey(roleName))
					{
						clusterStats.getAggregateStats().put(roleName, new HashMap<String, AggregateOverTime>());
					}

					Map<String, AggregateOverTime> aggregateOverTimeMap = clusterStats.getAggregateStats().get(roleName);

					genAvgCountHighLow(commonGroup, aggregateOverTimeMap);
				}
			}

			// Aggregate BatchMetaData
			List<BatchPeriodMapper> batchPeriodMappers = clusterService.getOrderedActiveBucketList();
			aggregateBatches(true, serverStats.getActiveBatches(), clusterStats, serverStats, batchPeriodMappers);
			aggregateBatches(false, serverStats.getActiveBackupBatches(), clusterStats, serverStats, batchPeriodMappers);

			// Bring in AggregatorsByPeriod
			List<AggregatorsByPeriod> aggregatorsByPeriods = channelCoordinator.getAggregatorsByPeriods();

			for (AggregatorsByPeriod aggregatorsByPeriod : aggregatorsByPeriods)
			{
				PeriodStatus periodStatus = new PeriodStatus();
				periodStatus.setPeriod(aggregatorsByPeriod.getPeriod().toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")));
				periodStatus.setPrimaryBackupSets(aggregatorsByPeriod.getPrimaryBackupSets());
				clusterStats.getPeriodStatuses().add(periodStatus);
			}
		}

		return clusterStats;
	}

	private void aggregateBatches(boolean primary, List<BatchMetaData> batchMetaDataList, ClusterStats clusterStats, ServerStats serverStats, List<BatchPeriodMapper> batchPeriodMappers)
	{
		for (BatchMetaData batchMetaData : batchMetaDataList)
		{
			BatchStatus batchStatus = new BatchStatus();
			batchStatus.setBatchMetaData(batchMetaData);
			batchStatus.setHostname(serverStats.getHostname());
			batchStatus.setWorkerId(serverStats.getWorkerId());
			batchStatus.setPrimary(primary);

			// Determine whether the batch is registered with the cluster service.
			for (BatchPeriodMapper batchPeriodMapper : batchPeriodMappers)
			{
				if (batchPeriodMapper.getChannelName().equals(batchMetaData.getChannelMetaData().getName()) &&
						batchPeriodMapper.getNearestPeriodCeiling().equals(batchMetaData.getNearestPeriodCeiling()))
				{
					batchStatus.setRegistered(true);
				}
			}

			String key = batchMetaData.getNearestPeriodCeiling().toString(DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss"));

			if (!clusterStats.getActiveBatches().containsKey(key))
			{
				clusterStats.getActiveBatches().put(key, new ArrayList<BatchStatus>());
			}

			clusterStats.getActiveBatches().get(key).add(batchStatus);
		}
	}

	private void genAvgCountHighLow(GroupStats groupStats, Map<String, AggregateOverTime> aggregateOverTimeMap)
	{
		if (groupStats.getAvgStats() != null)
		{
			for (OverTime<AvgStat> overTime : groupStats.getAvgStats().values())
			{
				// If the stat doesn't exist create it.
				if (!aggregateOverTimeMap.containsKey(overTime.getName()))
				{
					AggregateOverTime aggregateOverTime = new AggregateOverTime();
					aggregateOverTime.setName(overTime.getName());
					aggregateOverTime.setUnitOfMeasure(overTime.getUnitOfMeasure());
					aggregateOverTime.setAvgSec5(new AvgStat(overTime.getSec5().getTotal(), overTime.getSec5().getCount()));
					aggregateOverTime.setAvgMin1(new AvgStat(overTime.getMin1().getTotal(), overTime.getMin1().getCount()));
					aggregateOverTime.setAvgMin5(new AvgStat(overTime.getMin5().getTotal(), overTime.getMin5().getCount()));
					aggregateOverTime.setAvgMin30(new AvgStat(overTime.getMin30().getTotal(), overTime.getMin30().getCount()));
					aggregateOverTime.setAvgHour2(new AvgStat(overTime.getHour2().getTotal(), overTime.getHour2().getCount()));
					aggregateOverTime.setTotalSec5(overTime.getSec5().getAverage());
					aggregateOverTime.setTotalMin1(overTime.getMin1().getAverage());
					aggregateOverTime.setTotalMin5(overTime.getMin5().getAverage());
					aggregateOverTime.setTotalMin30(overTime.getMin30().getAverage());
					aggregateOverTime.setTotalHour2(overTime.getHour2().getAverage());
					aggregateOverTime.setHighSec5(overTime.getSec5().getAverage());
					aggregateOverTime.setHighMin1(overTime.getMin1().getAverage());
					aggregateOverTime.setHighMin5(overTime.getMin5().getAverage());
					aggregateOverTime.setHighMin30(overTime.getMin30().getAverage());
					aggregateOverTime.setHighHour2(overTime.getHour2().getAverage());
					aggregateOverTime.setLowSec5(overTime.getSec5().getAverage());
					aggregateOverTime.setLowMin1(overTime.getMin1().getAverage());
					aggregateOverTime.setLowMin5(overTime.getMin5().getAverage());
					aggregateOverTime.setLowMin30(overTime.getMin30().getAverage());
					aggregateOverTime.setLowHour2(overTime.getHour2().getAverage());
					aggregateOverTimeMap.put(overTime.getName(), aggregateOverTime);
				}
				else
				{
					AggregateOverTime aggregateOverTime = aggregateOverTimeMap.get(overTime.getName());

					// Update Averages
					aggregateOverTime.setAvgSec5(new AvgStat(overTime.getSec5().getTotal() + aggregateOverTime.getAvgSec5().getTotal(), overTime.getSec5().getCount() + aggregateOverTime.getAvgSec5().getCount()));
					aggregateOverTime.setAvgMin1(new AvgStat(overTime.getMin1().getTotal() + aggregateOverTime.getAvgMin1().getTotal(), overTime.getMin1().getCount() + aggregateOverTime.getAvgMin1().getCount()));
					aggregateOverTime.setAvgMin5(new AvgStat(overTime.getMin5().getTotal() + aggregateOverTime.getAvgMin5().getTotal(), overTime.getMin5().getCount() + aggregateOverTime.getAvgMin5().getCount()));
					aggregateOverTime.setAvgMin30(new AvgStat(overTime.getMin30().getTotal() + aggregateOverTime.getAvgMin30().getTotal(), overTime.getMin30().getCount() + aggregateOverTime.getAvgMin30().getCount()));
					aggregateOverTime.setAvgHour2(new AvgStat(overTime.getHour2().getTotal() + aggregateOverTime.getAvgHour2().getTotal(), overTime.getHour2().getCount() + aggregateOverTime.getAvgHour2().getCount()));

					// Update Totals
					aggregateOverTime.setTotalSec5(overTime.getSec5().getAverage() + aggregateOverTime.getTotalSec5());
					aggregateOverTime.setTotalMin1(overTime.getMin1().getAverage() + aggregateOverTime.getTotalMin1());
					aggregateOverTime.setTotalMin5(overTime.getMin5().getAverage() + aggregateOverTime.getTotalMin5());
					aggregateOverTime.setTotalMin30(overTime.getMin30().getAverage() + aggregateOverTime.getTotalMin30());
					aggregateOverTime.setTotalHour2(overTime.getHour2().getAverage() + aggregateOverTime.getTotalHour2());

					// Update Highs
					if (overTime.getSec5().getAverage() > aggregateOverTime.getHighSec5())
					{
						aggregateOverTime.setHighSec5(overTime.getSec5().getAverage());
					}
					if (overTime.getMin1().getAverage() > aggregateOverTime.getHighMin1())
					{
						aggregateOverTime.setHighMin1(overTime.getMin1().getAverage());
					}
					if (overTime.getMin5().getAverage() > aggregateOverTime.getHighMin5())
					{
						aggregateOverTime.setHighMin5(overTime.getMin5().getAverage());
					}
					if (overTime.getMin30().getAverage() > aggregateOverTime.getHighMin30())
					{
						aggregateOverTime.setHighMin30(overTime.getMin30().getAverage());
					}
					if (overTime.getHour2().getAverage() > aggregateOverTime.getHighHour2())
					{
						aggregateOverTime.setHighHour2(overTime.getHour2().getAverage());
					}

					// Update Lows
					if (overTime.getSec5().getAverage() < aggregateOverTime.getLowSec5())
					{
						aggregateOverTime.setLowSec5(overTime.getSec5().getAverage());
					}
					if (overTime.getMin1().getAverage() < aggregateOverTime.getLowMin1())
					{
						aggregateOverTime.setLowMin1(overTime.getMin1().getAverage());
					}
					if (overTime.getMin5().getAverage() < aggregateOverTime.getLowMin5())
					{
						aggregateOverTime.setLowMin5(overTime.getMin5().getAverage());
					}
					if (overTime.getMin30().getAverage() < aggregateOverTime.getLowMin30())
					{
						aggregateOverTime.setLowMin30(overTime.getMin30().getAverage());
					}
					if (overTime.getHour2().getAverage() < aggregateOverTime.getLowHour2())
					{
						aggregateOverTime.setLowHour2(overTime.getHour2().getAverage());
					}
				}
			}
		}

		if (groupStats.getCountStats() != null)
		{
			for (OverTime<Long> overTime : groupStats.getCountStats().values())
			{
				// If the stat doesn't exist create it.
				if (!aggregateOverTimeMap.containsKey(overTime.getName()))
				{
					AggregateOverTime aggregateOverTime = new AggregateOverTime();
					aggregateOverTime.setName(overTime.getName());
					aggregateOverTime.setUnitOfMeasure(overTime.getUnitOfMeasure());
					aggregateOverTime.setAvgSec5(new AvgStat(overTime.getSec5(), 1));
					aggregateOverTime.setAvgMin1(new AvgStat(overTime.getMin1(), 1));
					aggregateOverTime.setAvgMin5(new AvgStat(overTime.getMin5(), 1));
					aggregateOverTime.setAvgMin30(new AvgStat(overTime.getMin30(), 1));
					aggregateOverTime.setAvgHour2(new AvgStat(overTime.getHour2(), 1));
					aggregateOverTime.setTotalSec5(overTime.getSec5());
					aggregateOverTime.setTotalMin1(overTime.getMin1());
					aggregateOverTime.setTotalMin5(overTime.getMin5());
					aggregateOverTime.setTotalMin30(overTime.getMin30());
					aggregateOverTime.setTotalHour2(overTime.getHour2());
					aggregateOverTime.setHighSec5(overTime.getSec5());
					aggregateOverTime.setHighMin1(overTime.getMin1());
					aggregateOverTime.setHighMin5(overTime.getMin5());
					aggregateOverTime.setHighMin30(overTime.getMin30());
					aggregateOverTime.setHighHour2(overTime.getHour2());
					aggregateOverTime.setLowSec5(overTime.getSec5());
					aggregateOverTime.setLowMin1(overTime.getMin1());
					aggregateOverTime.setLowMin5(overTime.getMin5());
					aggregateOverTime.setLowMin30(overTime.getMin30());
					aggregateOverTime.setLowHour2(overTime.getHour2());
					aggregateOverTimeMap.put(overTime.getName(), aggregateOverTime);
				}
				else
				{
					AggregateOverTime aggregateOverTime = aggregateOverTimeMap.get(overTime.getName());

					// Update Averages
					aggregateOverTime.setAvgSec5(new AvgStat(overTime.getSec5() + aggregateOverTime.getAvgSec5().getTotal(), aggregateOverTime.getAvgSec5().getCount() + 1));
					aggregateOverTime.setAvgMin1(new AvgStat(overTime.getMin1() + aggregateOverTime.getAvgMin1().getTotal(), aggregateOverTime.getAvgMin1().getCount() + 1));
					aggregateOverTime.setAvgMin5(new AvgStat(overTime.getMin5() + aggregateOverTime.getAvgMin5().getTotal(), aggregateOverTime.getAvgMin5().getCount() + 1));
					aggregateOverTime.setAvgMin30(new AvgStat(overTime.getMin30() + aggregateOverTime.getAvgMin30().getTotal(), aggregateOverTime.getAvgMin30().getCount() + 1));
					aggregateOverTime.setAvgHour2(new AvgStat(overTime.getHour2() + aggregateOverTime.getAvgHour2().getTotal(), aggregateOverTime.getAvgHour2().getCount() + 1));

					// Update Totals
					aggregateOverTime.setTotalSec5(overTime.getSec5() + aggregateOverTime.getTotalSec5());
					aggregateOverTime.setTotalMin1(overTime.getMin1() + aggregateOverTime.getTotalMin1());
					aggregateOverTime.setTotalMin5(overTime.getMin5() + aggregateOverTime.getTotalMin5());
					aggregateOverTime.setTotalMin30(overTime.getMin30() + aggregateOverTime.getTotalMin30());
					aggregateOverTime.setTotalHour2(overTime.getHour2() + aggregateOverTime.getTotalHour2());

					// Update Highs
					if (overTime.getSec5() > aggregateOverTime.getHighSec5())
					{
						aggregateOverTime.setHighSec5(overTime.getSec5());
					}
					if (overTime.getMin1() > aggregateOverTime.getHighMin1())
					{
						aggregateOverTime.setHighMin1(overTime.getMin1());
					}
					if (overTime.getMin5() > aggregateOverTime.getHighMin5())
					{
						aggregateOverTime.setHighMin5(overTime.getMin5());
					}
					if (overTime.getMin30() > aggregateOverTime.getHighMin30())
					{
						aggregateOverTime.setHighMin30(overTime.getMin30());
					}
					if (overTime.getHour2() > aggregateOverTime.getHighHour2())
					{
						aggregateOverTime.setHighHour2(overTime.getHour2());
					}

					// Update Lows
					if (overTime.getSec5() < aggregateOverTime.getLowSec5())
					{
						aggregateOverTime.setLowSec5(overTime.getSec5());
					}
					if (overTime.getMin1() < aggregateOverTime.getLowMin1())
					{
						aggregateOverTime.setLowMin1(overTime.getMin1());
					}
					if (overTime.getMin5() < aggregateOverTime.getLowMin5())
					{
						aggregateOverTime.setLowMin5(overTime.getMin5());
					}
					if (overTime.getMin30() < aggregateOverTime.getLowMin30())
					{
						aggregateOverTime.setLowMin30(overTime.getMin30());
					}
					if (overTime.getHour2() < aggregateOverTime.getLowHour2())
					{
						aggregateOverTime.setLowHour2(overTime.getHour2());
					}
				}
			}
		}
	}

	private OverTime<AvgStat> getAvgStatByName(String groupName, String statName, ServerStats serverStats)
	{
		for (GroupStats groupStats : serverStats.getGroupStatsList().values())
		{
			if (groupStats.getName().equalsIgnoreCase(groupName))
			{
				for (OverTime<AvgStat> avgStat : groupStats.getAvgStats().values())
				{
					if (avgStat.getName().equalsIgnoreCase(statName))
					{
						return avgStat;
					}
				}
			}
		}

		return null;
	}
}
