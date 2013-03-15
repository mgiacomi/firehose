package com.gltech.scale.load;

import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.inbound.InboundRestClient;
import com.gltech.scale.pipeline.Pipeline;
import com.gltech.scale.pipeline.PipelineBuilder;
import com.gltech.scale.pipeline.Processor;
import com.gltech.scale.pipeline.Timed;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.storage.StorageServiceRestClient;
import com.gltech.scale.util.Props;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;

/**
 * This class will fire up various load scenarios from the client side
 * Should support:
 * A - multithreaded requests
 * B - variable byte payloads
 * C - variable sleep patterns
 * <p/>
 * Intended usage scenario:
 * If you want to test with a single high volume/low byte payload customers
 * and 10 low volume/high byte payload customers
 * You will want to run at least two instances of this application,
 * one client with multiple threads and a low sleep for HV/LB
 * and one client with many threads and high sleep for LV/HB
 * <p/>
 * LoadClient should also publish its results into ganglia.
 */
public class LoadClient
{
	private static final Logger logger = LoggerFactory.getLogger(LoadClient.class);

	private static Pipeline<TimedWork> pipeline;
	private static int bytes;
	private static InboundRestClient restClient;
	private static String bucketName;
	private static String payload;

	public static void main(String[] args) throws Exception
	{
		if (args.length != 3)
		{
			System.err.println("Expected usage: LoadClient <threads> <bytes> <sleep millis>");
			return;
		}
		int threads = Integer.parseInt(args[0]);
		bytes = Integer.parseInt(args[1]);
		int sleep = Integer.parseInt(args[2]);
		logger.info("starting client with {} threads and {} bytes", threads, bytes);

		payload = StringUtils.repeat("A", bytes);

		Props props = Props.getProps();
		props.loadFromFile(System.getProperty("user.dir") + "/load_client.properties");

		bucketName = "LoadBucket";
		restClient = new InboundRestClient();

		ServiceMetaData storageService = new ServiceMetaData();
		storageService.setListenAddress(props.get("storage_service.rest_host", "localhost"));
		storageService.setListenPort(props.get("storage_service.rest_port", 9090));

		try
		{
			new StorageServiceRestClient().putBucketMetaData(storageService,
					new ChannelMetaData("LoadClient", 10, false));
		}
		catch (Exception e)
		{
			// That's fine if it already exists.
		}


		pipeline = new PipelineBuilder(new LoadClientProcessor(), "LoadClient")
				.asynchronous(threads, 1000)
				.statistics("loadClient")
				.build();

		for (int i = 0; i < threads; i++)
		{
			pipeline.process(new TimedWork());
		}

	}

	static class LoadClientProcessor implements Processor<TimedWork>
	{
		public void process(TimedWork timedWork) throws Exception
		{
			Props props = Props.getProps();
			props.loadFromFile(System.getProperty("user.dir") + "/load_client.properties");

			//todo - gfm - 11/6/12 - insert an event via REST api
			ServiceMetaData eventService = new ServiceMetaData();
			eventService.setListenAddress(props.get("event_service.rest_host", "localhost"));
			eventService.setListenPort(props.get("event_service.rest_port", 9090));

			restClient.postEvent(eventService, "LoadClient", bucketName, payload);

			//todo - gfm - 11/6/12 - sleep where?

			//add another piece of work
			pipeline.process(new TimedWork());
		}
	}

	static class TimedWork implements Timed
	{
		long start;

		public void start()
		{
			start = System.nanoTime();
		}

		public long stop()
		{
			return System.nanoTime() - start;
		}
	}
}
