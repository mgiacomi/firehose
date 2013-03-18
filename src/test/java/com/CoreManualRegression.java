package com;

import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.inbound.InboundRestClient;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.core.server.EmbeddedServer;
import com.gltech.scale.util.ClientCreator;
import com.gltech.scale.util.ModelIO;
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
	static private ChannelMetaData bmd1 = new ChannelMetaData("Matt_1s", ChannelMetaData.TTL_DAY, false);
	static private ChannelMetaData bmd2 = new ChannelMetaData("Matt_2s", ChannelMetaData.TTL_DAY, false);
	static private ChannelMetaData bmd3 = new ChannelMetaData("Matt_3d", ChannelMetaData.TTL_DAY, false);

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

			ServiceMetaData inboundService = new ServiceMetaData();
			inboundService.setListenAddress(props.get("inbound.rest_host", Defaults.REST_HOST));
			inboundService.setListenPort(props.get("inbound.rest_port", Defaults.REST_PORT));

			InboundRestClient inboundRestClient = new InboundRestClient(new ModelIO());

			inboundRestClient.putChannelMetaData(inboundService, bmd1);
			inboundRestClient.putChannelMetaData(inboundService, bmd2);
			inboundRestClient.putChannelMetaData(inboundService, bmd3);
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
				postEvent("http://localhost:909" + new BigDecimal(rand.nextFloat()).setScale(0, BigDecimal.ROUND_HALF_UP), "Matt1s", String.valueOf(counter + "a"));
				postEvent("http://localhost:909" + new BigDecimal(rand.nextFloat()).setScale(0, BigDecimal.ROUND_HALF_UP), "Matt2s", String.valueOf(counter + "b"));
				postEvent("http://localhost:909" + new BigDecimal(rand.nextFloat()).setScale(0, BigDecimal.ROUND_HALF_UP), "Matt3d", String.valueOf(counter + "c"));
				Thread.sleep(5);
				counter++;
			}
		}

		static private void postEvent(String url, String channelName, String json)
		{
			WebResource webResource = client.resource(url + "/inbound/" + channelName);
			ClientResponse response = webResource.type(MediaType.APPLICATION_JSON_TYPE).post(ClientResponse.class, json);

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

			ServiceMetaData inbound = new ServiceMetaData();
			inbound.setListenAddress(props.get("inbound.rest_host", Defaults.REST_HOST));
			inbound.setListenPort(props.get("inbound.rest_port", Defaults.REST_PORT));

			InboundRestClient inboundRestClient = new InboundRestClient(new ModelIO());

			System.out.println("1s: " + inboundRestClient.getEvents(inbound, "Matt_1s", TimePeriodUtils.nearestPeriodCeiling(DateTime.now(), 5), TimeUnit.HOURS));
			System.out.println("2s: " + inboundRestClient.getEvents(inbound, "Matt_2s", TimePeriodUtils.nearestPeriodCeiling(DateTime.now(), 5), TimeUnit.HOURS));
			System.out.println("3d: " + inboundRestClient.getEvents(inbound, "Matt_3d", TimePeriodUtils.nearestPeriodCeiling(DateTime.now(), 5), TimeUnit.HOURS));
		}
	}
}
