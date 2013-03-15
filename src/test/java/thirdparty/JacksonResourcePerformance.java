package thirdparty;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.core.model.Message;
import com.gltech.scale.core.model.Batch;
import com.gltech.scale.core.model.ChannelMetaData;
import org.joda.time.DateTime;

import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

public class JacksonResourcePerformance
{
	private static final ObjectMapper textMapper = new ObjectMapper();

	private Batch getTimeBucket(int numEvents)
	{
//		ChannelMetaData channelMetaData = new ChannelMetaData("1", "2", ChannelMetaData.BucketType.eventset, 15, MediaType.APPLICATION_OCTET_STREAM_TYPE, ChannelMetaData.LifeTime.medium, ChannelMetaData.Redundancy.doublewritesync);
ChannelMetaData channelMetaData = null;
		Batch bigBatch = new Batch(channelMetaData, TimePeriodUtils.nearestPeriodCeiling(DateTime.now(), 5));
		for (int i = 0; i < numEvents; i++)
		{
			String payload = i + "asdf123asdf123asdf123asdf132asdf132asdf132a1sdf321asdf312adsf31asdf312adsf31asdf31asd";
			bigBatch.addEvent(new Message(1, payload.getBytes()));
		}

		return bigBatch;
	}

	private void printJVMMemStats()
	{
		long max = Runtime.getRuntime().maxMemory();
		long used = Runtime.getRuntime().totalMemory();
		System.out.println("jvm mem:  total " + (max / (1024 * 1024)) + "mb / used " + (used / (1024 * 1024)) + "mb / available " + ((max - used) / (1024 * 1024)) + "mb\n");
	}

	public void testJsonObjectMapperToString75000() throws Exception
	{
		printJVMMemStats();
		Batch batch = getTimeBucket(75000);
		System.out.println("TimeBucket with " + batch.getEvents().size() + " created.");
		long timer = System.currentTimeMillis();
		printJVMMemStats();

		ObjectNode objectNode = textMapper.createObjectNode();
//		objectNode.put("bucketMetaData", batch.getChannelMetaData().toJson());
		objectNode.put("bytes", batch.getBytes());
		objectNode.put("lastEventTime", batch.getLastEventTime().toString());

		ArrayNode dataNode = objectNode.putArray("data");

		for (Message message : batch.getEvents())
		{
//			dataNode.add(message.toJson());
		}
		System.out.println(batch.getEvents().size() + " items to JsonNode in " + (System.currentTimeMillis() - timer) + "ms");
		timer = System.currentTimeMillis();
		printJVMMemStats();

		objectNode.toString();
		System.out.println(batch.getEvents().size() + " JsonNode to string in " + (System.currentTimeMillis() - timer) + "ms");
		printJVMMemStats();
	}

	public void testObjectMapperOutputStream330000() throws Exception
	{
		printJVMMemStats();
		Batch batch = getTimeBucket(310000);
		System.out.println("TimeBucket with " + batch.getEvents().size() + " created.");
		long timer = System.currentTimeMillis();
		printJVMMemStats();

		ObjectNode objectNode = textMapper.createObjectNode();
//		objectNode.put("bucketMetaData", batch.getChannelMetaData().toJson());
		objectNode.put("bytes", batch.getBytes());
		objectNode.put("lastEventTime", batch.getLastEventTime().toString());

		ArrayNode dataNode = objectNode.putArray("data");

		for (Message message : batch.getEvents())
		{
//			dataNode.add(message.toJson());
		}
		System.out.println(batch.getEvents().size() + " items to JsonNode in " + (System.currentTimeMillis() - timer) + "ms");
		timer = System.currentTimeMillis();
		printJVMMemStats();

		JsonFactory f = textMapper.getJsonFactory();
		JsonGenerator g = f.createJsonGenerator(new FileOutputStream(new File("stream.json")));
		g.writeTree(objectNode);
		g.close();
		System.out.println(batch.getEvents().size() + " JsonNode to Stream to file " + (System.currentTimeMillis() - timer) + "ms");
		printJVMMemStats();
	}

	public void testJsonOutputStream650000() throws Exception
	{
		printJVMMemStats();
		Batch batch = getTimeBucket(650000);
		System.out.println("TimeBucket with " + batch.getEvents().size() + " created.");
		long timer = System.currentTimeMillis();
		printJVMMemStats();

		JsonFactory f = new JsonFactory();
		JsonGenerator g = f.createJsonGenerator(new FileOutputStream(new File("stream.json")));

		g.writeStartObject();
		g.writeFieldName("bucketMetaData");
		// You have to use write number to pipe in raw json see:
		// http://markmail.org/thread/xv26gqctvtee4uoo#query:+page:1+mid:m7ggc4syaj3vuwmq+state:results
//		g.writeNumber(batch.getChannelMetaData().toJson().toString());
		g.writeFieldName("bytes");
		g.writeNumber(batch.getBytes());
		g.writeStringField("lastEventTime", batch.getLastEventTime().toString());

		g.writeFieldName("data");
		g.writeStartArray();
		for (Message event : batch.getEvents())
		{
			g.writeStartObject();
//			g.writeStringField("customer", event.getCustomer());
//			g.writeStringField("bucket", event.getBucket());
			g.writeStringField("received_at", event.getReceived_at().toString());
			g.writeStringField("uuid", event.getUuid());
			g.writeBinaryField("payload", event.getPayload());
			g.writeEndObject();
		}
		g.writeEndArray();

		g.writeEndObject();
		g.close();
		System.out.println(batch.getEvents().size() + " items to OutputStream in " + (System.currentTimeMillis() - timer) + "ms");
		printJVMMemStats();
	}


	public void testJsonOutputStream100000000() throws Exception
	{
		printJVMMemStats();
		Batch batch = getTimeBucket(0);
		System.out.println("TimeBucket with " + batch.getEvents().size() + " created.");
		long timer = System.currentTimeMillis();
		printJVMMemStats();

		JsonFactory f = new JsonFactory();
		JsonGenerator g = f.createJsonGenerator(new FileOutputStream(new File("stream.json")));

		g.writeStartObject();
		g.writeFieldName("bucketMetaData");
		// You have to use write number to pipe in raw json see:
		// http://markmail.org/thread/xv26gqctvtee4uoo#query:+page:1+mid:m7ggc4syaj3vuwmq+state:results
//		g.writeNumber(batch.getChannelMetaData().toJson().toString());
		g.writeFieldName("bytes");
		g.writeNumber(batch.getBytes());
		g.writeStringField("lastEventTime", batch.getLastEventTime().toString());

		g.writeFieldName("data");
		g.writeStartArray();

		for (int i = 0; i < 100000000; i++)
		{
			String payload = i + "asdf123asdf123asdf123asdf132asdf132asdf132a1sdf321asdf312adsf31asdf312adsf31asdf31asd";
			Message event = new Message(1, payload.getBytes());

			g.writeStartObject();
//			g.writeStringField("customer", event.getCustomer());
//			g.writeStringField("bucket", event.getBucket());
			g.writeStringField("received_at", event.getReceived_at().toString());
			g.writeStringField("uuid", event.getUuid());
			g.writeBinaryField("payload", event.getPayload());
			g.writeEndObject();
		}
		g.writeEndArray();

		g.writeEndObject();
		g.close();
		System.out.println(batch.getEvents().size() + " items to OutputStream in " + (System.currentTimeMillis() - timer) + "ms");
		printJVMMemStats();
	}

	public void testJsonOutputStreamBinaryVsText1000000() throws Exception
	{
		printJVMMemStats();
		Batch batch = getTimeBucket(500000);
		System.out.println("TimeBucket with " + batch.getEvents().size() + " created.");
		long timer = System.currentTimeMillis();
		printJVMMemStats();

		List<JsonGenerator> generators = new ArrayList<>();

		JsonFactory f = new JsonFactory();
		generators.add(f.createJsonGenerator(new FileOutputStream(new File("stream.json.text"))));

		SmileFactory smile = new SmileFactory();
		generators.add(smile.createJsonGenerator(new FileOutputStream(new File("stream.json.smile"))));

		for (JsonGenerator g : generators)
		{
			g.writeStartObject();
			g.writeFieldName("bytes");
			g.writeNumber(batch.getBytes());
			g.writeStringField("lastEventTime", batch.getLastEventTime().toString());

			g.writeFieldName("data");
			g.writeStartArray();
			for (Message event : batch.getEvents())
			{
				g.writeStartObject();
//				g.writeStringField("customer", event.getCustomer());
//				g.writeStringField("bucket", event.getBucket());
				g.writeStringField("received_at", event.getReceived_at().toString());
				g.writeStringField("uuid", event.getUuid());
				g.writeBinaryField("payload", event.getPayload());
				g.writeEndObject();
			}
			g.writeEndArray();

			g.writeEndObject();
			g.close();
			System.out.println(g.getClass().getSimpleName() + " : " + batch.getEvents().size() + " items to OutputStream in " + (System.currentTimeMillis() - timer) + "ms");
			printJVMMemStats();
		}
	}
}
