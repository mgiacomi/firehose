package com.gltech.scale.core.storage;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gltech.scale.core.monitor.Timer;
import com.gltech.scale.core.storage.bytearray.StoragePayload;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

public class StoragePayloadPerformance
{
	private static final ObjectMapper mapper = new ObjectMapper();
	private static final Logger logger = LoggerFactory.getLogger("com.lokiscale.storage.StoragePayloadPerformance");
	private static final JsonFactory jsonFactory = new JsonFactory();
	//todo - gfm - 10/4/12 - write some performance tests for reading & writing StoragePayloads

	public static void main(String[] args) throws IOException
	{
		String payload = StringUtils.repeat("A", 1000 * 1024);
		StoragePayload storagePayload = new StoragePayload(UUID.randomUUID().toString(), "someCustomer", "someBucket", payload.getBytes());
		//testObjectMapper(storagePayload);
		testStreaming(storagePayload);
	}

	/**
	 * 256 MB memory, Java 1.7, OSX 10.8
	 * 256 bytes, 135 ms per 10K iterations
	 * 512 bytes, 220 ms per 10K iterations
	 * 1024 bytes, 355 ms per 10K iterations
	 * 5 KB, 1750 ms per 10K iterations
	 * 10 KB, 3560 ms per 10K iterations
	 * 50 KB, 16600 ms per 10K iterations
	 * 100 KB, 34,200 ms per 10K iterations
	 * 1000 KB, 405,000 ms per 10K iterations
	 * 5000 KB, 2,700,000 ms per 10K iterations
	 */
	public static void testObjectMapper(StoragePayload storagePayload) throws IOException
	{

		logger.info("payload size " + storagePayload.getPayload().length);
		for (int i = 0; i < 100; i++)
		{
			doLoop(storagePayload);
		}
	}

	private static void doLoop(StoragePayload storagePayload) throws IOException
	{
		Timer timer = new Timer();
		timer.start();
		for (int i = 0; i < 100; i++)
		{
			writeAndRead(storagePayload);
		}
		timer.stop();
		logger.info("timer " + timer);
	}

	private static void writeAndRead(StoragePayload storagePayload) throws IOException
	{
		ObjectNode node = mapper.createObjectNode();
		node.put("customer", storagePayload.getCustomer());
		node.put("bucket", storagePayload.getBucket());
		node.put("id", storagePayload.getId());
		node.put("created", storagePayload.getCreated_at().getMillis());
		node.put("payload", storagePayload.getPayload());
		String nodeString = node.toString();
		JsonNode readNode = mapper.readTree(nodeString);
		StoragePayload parsed = new StoragePayload(
				readNode.get("id").asText(),
				readNode.get("customer").asText(),
				readNode.get("bucket").asText(),
				readNode.get("payload").binaryValue()
		);
		parsed.setCreated_at(new DateTime(readNode.get("created").asLong()));
	}


	/**
	 * 256 MB memory, Java 1.7, OSX 10.8
	 * 256 bytes, 64 ms per 10K iterations
	 * 1024 bytes, 190 ms per 10K iterations
	 * 10 KB, 1650 ms per 10K iterations
	 * 100 KB, 17200 ms per 10K iterations
	 * 1000 KB, 177000 ms per 10K iterations
	 */
	public static void testStreaming(StoragePayload storagePayload) throws IOException
	{

		logger.info("payload size " + storagePayload.getPayload().length);
		for (int i = 0; i < 100; i++)
		{
			doStreamingLoop(storagePayload);
		}
	}

	private static void doStreamingLoop(StoragePayload storagePayload) throws IOException
	{
		Timer timer = new Timer();
		timer.start();
		for (int i = 0; i < 100; i++)
		{
			stream(storagePayload);
		}
		timer.stop();
		logger.info("timer " + timer);
	}

	public static void stream(StoragePayload storagePayload) throws IOException
	{
		StoragePayload.convert(storagePayload.convert());

	}


/*
	public static void stream(StoragePayload storagePayload) throws IOException
	{
		//todo - gfm - 10/9/12 - test with NIO
		//todo - gfm - 10/9/12 - what about versioning?
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		JsonGenerator generator = jsonFactory.createJsonGenerator(baos);
		generator.writeStartObject();
		generator.writeStringField("id", storagePayload.getId());
		generator.writeStringField("customer", storagePayload.getCustomer());
		generator.writeStringField("bucket", storagePayload.getBucket());
		generator.writeBinaryField("payload", storagePayload.getPayload());
		generator.writeNumberField("created", storagePayload.getCreated_at().getMillis());
		generator.writeEndObject();
		generator.close();
		byte[] bytes = baos.toByteArray();
		JsonParser parser = jsonFactory.createJsonParser(bytes);

		StoragePayload parsed = new StoragePayload(parser.nextTextValue(), parser.nextTextValue(),
				parser.nextTextValue(), parser.nextValue().asByteArray());

		parsed.setCreated_at(new DateTime(parser.nextLongValue(0)));
		baos.close();
		parser.close();

	}
*/
}
