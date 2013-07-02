package com.gltech.scale.core.server;

import com.gltech.scale.core.aggregator.Batch;
import com.gltech.scale.core.aggregator.BatchMemory;
import com.gltech.scale.core.aggregator.BatchNIOFile;
import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.util.ClientCreator;
import com.gltech.scale.util.Props;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.joda.time.DateTime;

import javax.ws.rs.core.MediaType;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

public class LoadClient
{
	static public class Test
	{
		static private final Client client = ClientCreator.createCached();

		public static void main(String[] args) throws Exception
		{
			int counter = 0;
			while (true)
			{
				//postEvent("http://" + args[0] + ":" + args[1], "fast", String.valueOf(counter + "a"));
				//postEvent("http://" + args[0] + ":" + args[1], "redundant", String.valueOf(counter + "b"));
				httpPost("http://" + args[0] + ":" + args[1], "fast", String.valueOf(counter + "a"));
				httpPost("http://" + args[0] + ":" + args[1], "redundant", String.valueOf(counter + "b"));
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

		public static String httpPost(String urlStr, String channelName, String json) throws Exception
		{
			URL url = new URL(urlStr + "/inbound/" + channelName);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setDoOutput(true);
			conn.setDoInput(true);
			conn.setUseCaches(false);
			conn.setAllowUserInteraction(false);
			conn.setRequestProperty("Content-Type", "application/json");

			// Create the form content
			OutputStream out = conn.getOutputStream();
			Writer writer = new OutputStreamWriter(out);
			writer.write(json);
			writer.close();
			out.close();

			if (conn.getResponseCode() != 202)
			{
				throw new IOException(conn.getResponseMessage());
			}

			// Buffer the result into a string
			BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			StringBuilder sb = new StringBuilder();
			String line;
			while ((line = rd.readLine()) != null)
			{
				sb.append(line);
			}
			rd.close();

			conn.disconnect();
			return sb.toString();
		}
	}

	static public class MemoryChannel
	{
		public static void main(String[] args) throws Exception
		{
			ChannelMetaData channelMetaData = new ChannelMetaData("1", ChannelMetaData.TTL_DAY, true);
			Batch batch = new BatchMemory(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(DateTime.now(), 5));

			String str = "lafjs;lakjsd;flkja;sdlfja;lsdfkjoiwuep2 rupqnfaswd cnh29r3nu p2 fhwn9\t2rnu9\t2pfwjp9\t2rn9\t2pmr \t2p98\t9Y84\t293RC9M2 RP9U \tN p9qfmoiqvhwnfmqiufoqinhmoiqhucmflafjs;lakjsd;flkja;sdlfja;lsdfkjoiwuep2 rupqnfaswd cnh29r3nu p2 fhwn9\t2rnu9\t2pfwjp9\t2rn9\t2pmr \t2p98\t9Y84\t293RC9M2 RP9U \tN p9qfmoiqvhwnfmqiufoqinhmoiqhucmflafjs;lakjsd;flkja;sdlfja;lsdfkjoiwuep2 rupqnfaswd cnh29r3nu p2 fhwn9\t2rnu9\t2pfwjp9\t2rn9\t2pmr \t2p98\t9Y84\t293RC9M2 RP9U \tN p9qfmoiqvhwnfmqiufoqinhmoiqhucmflafjs;lakjsd;flkja;sdlfja;lsdfkjoiwuep2 rupqnfaswd cnh29r3nu p2 fhwn9\t2rnu9\t2pfwjp9\t2rn9\t2pmr \t2p98\t9Y84\t293RC9M2 RP9U \tN p9qfmoiqvhwnfmqiufoqinhmoiqhucmflafjs;lakjsd;flkja;sdlfja;lsdfkjoiwuep2 rupqnfaswd cnh29r3nu p2 fhwn9\t2rnu9\t2pfwjp9\t2rn9\t2pmr \t2p98\t9Y84\t293RC9M2 RP9U \tN p9qfmoiqvhwnfmqiufoqinhmoiqhucmflafjs;lakjsd;flkja;sdlfja;lsdfkjoiwuep2 rupqnfaswd cnh29r3nu p2 fhwn9\t2rnu9\t2pfwjp9\t2rn9\t2pmr \t2p98\t9Y84\t293RC9M2 RP9U \tN p9qfmoiqvhwnfmqiufoqinhmoiqhucmflafjs;lakjsd;flkja;sdlfja;lsdfkjoiwuep2 rupqnfaswd cnh29r3nu p2 fhwn9\t2rnu9\t2pfwjp9\t2rn9\t2pmr \t2p98\t9Y84\t293RC9M2 RP9U \tN p9qfmoiqvhwnfmqiufoqinhmoiqhucmflafjs;lakjsd;flkja;sdlfja;lsdfkjoiwuep2 rupqnfaswd cnh29r3nu p2 fhwn9\t2rnu9\t2pfwjp9\t2rn9\t2pmr \t2p98\t9Y84\t293RC9M2 RP9U \tN p9qfmoiqvhwnfmqiufoqinhmoiqhucmflafjs;lakjsd;flkja;sdlfja;lsdfkjoiwuep2 rupqnfaswd cnh29r3nu p2 fhwn9\t2rnu9\t2pfwjp9\t2rn9\t2pmr \t2p98\t9Y84\t293RC9M2 RP9U \tN p9qfmoiqvhwnfmqiufoqinhmoiqhucmf";
			System.out.println("message size: " + str.getBytes().length);

			long timer = System.currentTimeMillis();
			for (int i = 0; i < 500000; i++)
			{
				batch.addMessage(str.getBytes());
			}
			System.out.println("completed: " + (System.currentTimeMillis() - timer) + "ms");
		}
	}

	static public class FileChannel
	{
		public static void main(String[] args) throws Exception
		{
			Props props = Props.getProps();
			props.set("channel_file_dir", args[0]);

			ChannelMetaData channelMetaData = new ChannelMetaData("1", ChannelMetaData.TTL_DAY, true);
			Batch batch = new BatchNIOFile(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(DateTime.now(), 5));

			String str = "lafjs;lakjsd;flkja;sdlfja;lsdfkjoiwuep2 rupqnfaswd cnh29r3nu p2 fhwn9\t2rnu9\t2pfwjp9\t2rn9\t2pmr \t2p98\t9Y84\t293RC9M2 RP9U \tN p9qfmoiqvhwnfmqiufoqinhmoiqhucmflafjs;lakjsd;flkja;sdlfja;lsdfkjoiwuep2 rupqnfaswd cnh29r3nu p2 fhwn9\t2rnu9\t2pfwjp9\t2rn9\t2pmr \t2p98\t9Y84\t293RC9M2 RP9U \tN p9qfmoiqvhwnfmqiufoqinhmoiqhucmflafjs;lakjsd;flkja;sdlfja;lsdfkjoiwuep2 rupqnfaswd cnh29r3nu p2 fhwn9\t2rnu9\t2pfwjp9\t2rn9\t2pmr \t2p98\t9Y84\t293RC9M2 RP9U \tN p9qfmoiqvhwnfmqiufoqinhmoiqhucmflafjs;lakjsd;flkja;sdlfja;lsdfkjoiwuep2 rupqnfaswd cnh29r3nu p2 fhwn9\t2rnu9\t2pfwjp9\t2rn9\t2pmr \t2p98\t9Y84\t293RC9M2 RP9U \tN p9qfmoiqvhwnfmqiufoqinhmoiqhucmflafjs;lakjsd;flkja;sdlfja;lsdfkjoiwuep2 rupqnfaswd cnh29r3nu p2 fhwn9\t2rnu9\t2pfwjp9\t2rn9\t2pmr \t2p98\t9Y84\t293RC9M2 RP9U \tN p9qfmoiqvhwnfmqiufoqinhmoiqhucmflafjs;lakjsd;flkja;sdlfja;lsdfkjoiwuep2 rupqnfaswd cnh29r3nu p2 fhwn9\t2rnu9\t2pfwjp9\t2rn9\t2pmr \t2p98\t9Y84\t293RC9M2 RP9U \tN p9qfmoiqvhwnfmqiufoqinhmoiqhucmflafjs;lakjsd;flkja;sdlfja;lsdfkjoiwuep2 rupqnfaswd cnh29r3nu p2 fhwn9\t2rnu9\t2pfwjp9\t2rn9\t2pmr \t2p98\t9Y84\t293RC9M2 RP9U \tN p9qfmoiqvhwnfmqiufoqinhmoiqhucmflafjs;lakjsd;flkja;sdlfja;lsdfkjoiwuep2 rupqnfaswd cnh29r3nu p2 fhwn9\t2rnu9\t2pfwjp9\t2rn9\t2pmr \t2p98\t9Y84\t293RC9M2 RP9U \tN p9qfmoiqvhwnfmqiufoqinhmoiqhucmflafjs;lakjsd;flkja;sdlfja;lsdfkjoiwuep2 rupqnfaswd cnh29r3nu p2 fhwn9\t2rnu9\t2pfwjp9\t2rn9\t2pmr \t2p98\t9Y84\t293RC9M2 RP9U \tN p9qfmoiqvhwnfmqiufoqinhmoiqhucmf";
			System.out.println("message size: " + str.getBytes().length);

			long timer = System.currentTimeMillis();
			for (int i = 0; i < 500000; i++)
			{
				batch.addMessage(str.getBytes());
			}
			System.out.println("completed: " + (System.currentTimeMillis() - timer) + "ms");
		}
	}


}