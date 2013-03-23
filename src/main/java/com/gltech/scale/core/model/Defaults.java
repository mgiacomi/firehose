package com.gltech.scale.core.model;

public class Defaults
{
	static public final String REST_HOST = "localhost";
	static public final int MEGABYTES = 1024 * 1024;
	static public final int PERIOD_SECONDS = 5;
	static public final int MAX_PAYLOAD_SIZE_KB = 50;
	static public final int REST_PORT = 9090;
	static public final int CONCURRENT_STORE_WRITES = 10;
	static public final int STORAGE_WRITER_CHECK_FOR_WORK_INTERVAL_SECS = 5;
	static public final int STORAGE_WRITER_ACTIVE_WRITERS = 100;
	static public final int STORAGE_WRITER_WAIT_FOR_SHUTDOWN_MINS = 5;
	static public final boolean STORAGE_WRITER_CLEAN_SHUTDOWN = true;
	static public final String STORAGE_STORE = "voldemort";
	static public final int WEIGHT_MANGER_SLEEP_MILLIS = 500;
	static public final int STATS_MANAGER_CLEANUP_SLEEP_MINS = 5;
}
