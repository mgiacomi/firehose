package com;

import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.inbound.InboundRestClient;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.server.EmbeddedServer;
import com.gltech.scale.core.storage.*;
import com.gltech.scale.util.ClientCreator;
import com.gltech.scale.util.Props;
import com.netflix.curator.test.TestingServer;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.joda.time.DateTime;

import javax.ws.rs.core.MediaType;
import java.math.BigDecimal;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class CoreManualRegression
{
	static private final Client client = ClientCreator.createCached();
	static private ChannelMetaData bmd1 = new ChannelMetaData("Matt", "1s", ChannelMetaData.BucketType.eventset, 5, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
	static private ChannelMetaData bmd2 = new ChannelMetaData("Matt", "2s", ChannelMetaData.BucketType.eventset, 30, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
	static private ChannelMetaData bmd3 = new ChannelMetaData("Matt", "3d", ChannelMetaData.BucketType.eventset, 60, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.doublewritesync);

	static public class PrimaryServer
	{
		public static void main(String[] args) throws Exception
		{
			Props props = Props.getProps();
			props.loadFromFile(System.getProperty("user.dir") + "/src/test/resources/primary_server.properties");

			TestingServer testingServer = new TestingServer(21818);

			EmbeddedServer.start(9090);

			while (true)
			{
				Thread.sleep(60000);
			}
		}

		//EmbeddedServer.stop();
		//testingServer.stop();
	}

	static public class StartSecondServer
	{
		public static void main(String[] args) throws Exception
		{
			Props props = Props.getProps();
			props.loadFromFile(System.getProperty("user.dir") + "/src/test/resources/second_server.properties");

			EmbeddedServer.start(9091);

			while (true)
			{
				Thread.sleep(60000);
			}

			//EmbeddedServer.stop();
		}
	}

	static public class CreateBuckets
	{
		public static void main(String[] args) throws Exception
		{
			Props props = Props.getProps();
			props.loadFromFile(System.getProperty("user.dir") + "/src/test/resources/props.properties");

			ServiceMetaData storageService = new ServiceMetaData();
			storageService.setListenAddress(props.get("event_service.rest_host", "localhost"));
			storageService.setListenPort(props.get("event_service.rest_port", 9090));

			StorageServiceRestClient storageServiceRestClient = new StorageServiceRestClient();

			storageServiceRestClient.putBucketMetaData(storageService, bmd1);
			storageServiceRestClient.putBucketMetaData(storageService, bmd2);
			storageServiceRestClient.putBucketMetaData(storageService, bmd3);
		}
	}

	static public class PumpInEvents
	{
		public static void main(String[] args) throws Exception
		{
			Props props = Props.getProps();
			props.loadFromFile(System.getProperty("user.dir") + "/src/test/resources/props.properties");

			Random rand = new Random();

			int counter = 0;
			while (true)
			{
				postEvent("http://localhost:909" + new BigDecimal(rand.nextFloat()).setScale(0, BigDecimal.ROUND_HALF_UP), "Matt", "1s", String.valueOf(counter + "a"));
				postEvent("http://localhost:909" + new BigDecimal(rand.nextFloat()).setScale(0, BigDecimal.ROUND_HALF_UP), "Matt", "2s", String.valueOf(counter + "b"));
				postEvent("http://localhost:909" + new BigDecimal(rand.nextFloat()).setScale(0, BigDecimal.ROUND_HALF_UP), "Matt", "3d", String.valueOf(counter + "c"));
				Thread.sleep(5);
				counter++;
			}
		}

		static private void postEvent(String url, String customer, String bucket, String json)
		{
			WebResource webResource = client.resource(url + "/events/" + customer + "/" + bucket);
			ClientResponse response = webResource.type("application/json").post(ClientResponse.class, json);

			if (response.getStatus() != 202)
			{
				throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
			}
		}
	}

	static public class QueryEvents
	{
		public static void main(String[] args) throws Exception
		{
			Props props = Props.getProps();
			props.loadFromFile(System.getProperty("user.dir") + "/src/test/resources/props.properties");

			ServiceMetaData eventService = new ServiceMetaData();
			eventService.setListenAddress(props.get("event_service.rest_host", "localhost"));
			eventService.setListenPort(props.get("event_service.rest_port", 9090));

			InboundRestClient inboundRestClient = new InboundRestClient();

			System.out.println("1s: " + inboundRestClient.getEvents(eventService, "Matt", "1s", TimePeriodUtils.nearestPeriodCeiling(DateTime.now(), 5), TimeUnit.HOURS));
			System.out.println("2s: " + inboundRestClient.getEvents(eventService, "Matt", "2s", TimePeriodUtils.nearestPeriodCeiling(DateTime.now(), 5), TimeUnit.HOURS));
			System.out.println("3d: " + inboundRestClient.getEvents(eventService, "Matt", "3d", TimePeriodUtils.nearestPeriodCeiling(DateTime.now(), 5), TimeUnit.HOURS));
		}
	}
}
