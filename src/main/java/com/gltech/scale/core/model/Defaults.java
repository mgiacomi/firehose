package com.gltech.scale.core.model;

import com.dyuproject.protostuff.runtime.ExplicitIdStrategy;
import com.gltech.scale.core.aggregator.PrimaryBackupSet;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.stats.results.AvgStat;
import com.gltech.scale.core.stats.results.GroupStats;
import com.gltech.scale.core.stats.results.OverTime;
import com.gltech.scale.monitoring.model.*;

public class Defaults
{
	// Core
	static public final long KBytes = 1024L;
	static public final int MEGABYTES = 1024 * 1024;
	static public final String REST_HOST = "localhost";
	static public final int REST_PORT = 9090;
	static public final int PERIOD_SECONDS = 1;
	static public final int MAX_PAYLOAD_SIZE_KB = 333;
	static public final int CONCURRENT_STORE_WRITES = 10;
	static public final int STORAGE_WRITER_ACTIVE_WRITERS = 100;
	static public final int STORAGE_WRITER_WAIT_FOR_SHUTDOWN_MINS = 5;
	static public final String STORAGE_STORE = "voldemort";
	static public final int WEIGHT_MANGER_REGISTER_EVERY_X_MILLIS = 500;
	static public final int STATS_MANAGER_CLEANUP_RUN_EVERY_X_MINS = 5;
	static public final int STATS_MANAGER_CALLBACK_RUN_EVERY_X_SECONDS = 5;

	// Monitoring
	static public final int GATHER_SERVICE_RUN_EVERY_X_SECONDS = 3;

	public static void registerProtoClasses()
	{
		ExplicitIdStrategy.Registry registry = new ExplicitIdStrategy.Registry();
		registry.registerPojo(Message.class, 1);
		registry.registerPojo(Batch.class, 2);
		registry.registerPojo(ChannelMetaData.class, 3);
		registry.registerPojo(GroupStats.class, 4);
		registry.registerPojo(AvgStat.class, 5);
		registry.registerPojo(OverTime.class, 6);
		registry.registerPojo(ServerStats.class, 7);
		registry.registerPojo(AggregateOverTime.class, 8);
		registry.registerPojo(ClusterStats.class, 9);
		registry.registerPojo(BatchStatus.class, 10);
		registry.registerPojo(ServiceMetaData.class, 11);
		registry.registerPojo(PrimaryBackupSet.class, 12);
		registry.registerPojo(PeriodStatus.class, 13);
	}
}
