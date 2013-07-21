package com;

import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.inbound.InboundRestClient;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.core.outbound.OutboundRestClient;
import com.gltech.scale.core.server.EmbeddedServer;
import com.gltech.scale.util.RestClientCreator;
import com.gltech.scale.core.model.ModelIO;
import com.gltech.scale.util.Props;
import com.gltech.scale.voldemort.VoldemortTestUtil;
import com.netflix.curator.test.TestingServer;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.joda.time.DateTime;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import java.math.BigDecimal;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class CoreManualRegression
{
	static private final Client client = RestClientCreator.createCached();
	static private ChannelMetaData bmd1 = new ChannelMetaData("fast", ChannelMetaData.TTL_DAY, false);
	static private ChannelMetaData bmd2 = new ChannelMetaData("redundant", ChannelMetaData.TTL_DAY, true);

	static public class PrimaryServer
	{
		public static void main(String[] args) throws Exception
		{
			Props props = Props.getProps();
			props.loadFromFile(System.getProperty("user.dir") + "/src/test/resources/primary_server.properties");

			TestingServer testingServer = new TestingServer(21818);
			//VoldemortTestUtil.start();

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

	static public class MonitoringInstance
	{
		public static void main(String[] args) throws Exception
		{
			Props props = Props.getProps();
			props.loadFromFile(System.getProperty("user.dir") + "/src/test/resources/monitoring.properties");

			com.gltech.scale.monitoring.server.EmbeddedServer.start(9292);

			while (true)
			{
				Thread.sleep(60000);
			}
		}
	}

	static public class CreateBuckets
	{
		public static void main(String[] args) throws Exception
		{
			Props props = Props.getProps();
			props.loadFromFile(System.getProperty("user.dir") + "/src/test/resources/props.properties");

			ServiceMetaData inboundService = new ServiceMetaData();
			inboundService.setListenAddress("localhost");
			inboundService.setListenPort(9090);

			InboundRestClient inboundRestClient = new InboundRestClient(new ModelIO());

			inboundRestClient.putChannelMetaData(inboundService, bmd1);
			inboundRestClient.putChannelMetaData(inboundService, bmd2);
		}
	}

	static public class TestWithParams
	{
		public static void main(String[] args) throws Exception
		{
			MultivaluedMap<String,String> queryParams = new MultivaluedMapImpl();
			queryParams.add("name1", "val1");
			queryParams.add("name2", "val2");

			WebResource webResource = client.resource("http://localhost:9090/inbound/test");
			ClientResponse response = webResource.queryParams(queryParams).type(MediaType.APPLICATION_JSON_TYPE).post(ClientResponse.class, "{}");

			if (response.getStatus() != 202)
			{
				throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
			}
		}
	}

	static public class PumpInEvents
	{
		public static void main(String[] args) throws Exception
		{
			try
			{
//				CreateBuckets.main(null);
			}
			catch (Exception e)
			{
			}

			Props props = Props.getProps();
			props.loadFromFile(System.getProperty("user.dir") + "/src/test/resources/props.properties");

			Random rand = new Random();

			int counter = 0;
			while (true)
			{
				postEvent("http://localhost:9090", "fast", String.valueOf(counter + "a"));
//				postEvent("http://localhost:9090", "redundant", String.valueOf(counter + "b"));
//				postEvent("http://localhost:909" + new BigDecimal(rand.nextFloat()).setScale(0, BigDecimal.ROUND_HALF_UP), "fast", String.valueOf(counter + "a"));
//				postEvent("http://localhost:909" + new BigDecimal(rand.nextFloat()).setScale(0, BigDecimal.ROUND_HALF_UP), "redundant", String.valueOf(counter + "b"));
//				Thread.sleep(5);
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

			OutboundRestClient outboundRestClient = new OutboundRestClient(new ModelIO());

			System.out.println("fast: " + outboundRestClient.getMessages(inbound, "fast", TimePeriodUtils.nearestPeriodCeiling(DateTime.now(), 5), TimeUnit.HOURS));
			System.out.println("redundant: " + outboundRestClient.getMessages(inbound, "redundant", TimePeriodUtils.nearestPeriodCeiling(DateTime.now(), 5), TimeUnit.HOURS));
		}
	}
}
