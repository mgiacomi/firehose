package com.gltech.scale.core.model;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class Batch
{
	private static final Logger logger = LoggerFactory.getLogger(Batch.class);
	private DateTime nearestPeriodCeiling;
	private DateTime firstEventTime;
	private DateTime lastEventTime = DateTime.now();
	private LinkedBlockingQueue<Message> data = new LinkedBlockingQueue<>();
	private AtomicLong bytes = new AtomicLong(0);
	private ChannelMetaData channelMetaData;

	public Batch(ChannelMetaData channelMetaData, DateTime nearestPeriodCeiling)
	{
		this.channelMetaData = channelMetaData;
		this.nearestPeriodCeiling = nearestPeriodCeiling;
	}

	static private List<Message> jsonToEvents(JsonParser jp) throws IOException
	{
		List<Message> events = new ArrayList<>();

		while (jp.nextToken() != JsonToken.END_ARRAY)
		{
//			events.add(Message.jsonToEvent(jp));
		}

		return events;
	}

	static public List<Message> jsonToEvents(InputStream in)
	{
		List<Message> events;

		try
		{
			JsonFactory f = new MappingJsonFactory();
			JsonParser jp = f.createJsonParser(in);

			jp.nextToken();
			events = jsonToEvents(jp);
			jp.close();
		}
		catch (IOException e)
		{
			logger.warn("unable to parse json", e);
			throw new RuntimeException("unable to parse json", e);
		}

		return events;
	}

	public void toJson(OutputStream out)
	{
		try
		{
			JsonFactory f = new JsonFactory();
			JsonGenerator g = f.createJsonGenerator(out);

			// You have to use write number to pipe in raw json see:
			// http://markmail.org/thread/xv26gqctvtee4uoo#query:+page:1+mid:m7ggc4syaj3vuwmq+state:results

			g.writeStartObject();
			g.writeFieldName("bucketMetaData");
			g.writeFieldName("bytes");
			g.writeNumber(bytes.get());
			g.writeFieldName("eventsAdded");
			g.writeStringField("lastEventTime", lastEventTime.toString());
			g.writeStringField("firstEventTime", firstEventTime.toString());
			g.writeStringField("nearestPeriodCeiling", nearestPeriodCeiling.toString());

			g.writeFieldName("data");
			eventsToJson(g, new ArrayList<>(data));

			g.writeEndObject();
			g.close();
		}
		catch (IOException e)
		{
			throw new RuntimeException("Failed to convert Timebucket to JSON", e);
		}
	}

	public void eventsToJson(OutputStream out)
	{
		try
		{
			JsonFactory f = new JsonFactory();
			JsonGenerator g = f.createJsonGenerator(out);

			eventsToJson(g, new ArrayList<>(data));

			g.close();
		}
		catch (IOException e)
		{
			throw new RuntimeException("Failed to convert TimeBucket events to JSON", e);
		}
	}

	static public void eventsToJson(List<Message> events, OutputStream outputStream) throws IOException
	{
		try
		{
			JsonFactory f = new JsonFactory();
			JsonGenerator g = f.createJsonGenerator(outputStream);
			eventsToJson(g, events);
			g.close();
		}
		catch (IOException e)
		{
			throw new RuntimeException("Failed to convert events to JSON", e);
		}
	}

	static private void eventsToJson(JsonGenerator g, List<Message> events) throws IOException
	{
		g.writeStartArray();
		for (Message event : events)
		{
			g.writeStartObject();
//			g.writeStringField("customer", event.getCustomer());
//			g.writeStringField("bucket", event.getBucket());
			g.writeStringField("received_at", event.getReceived_at().toString());
			g.writeStringField("uuid", event.getUuid());
			g.writeBinaryField("payload", event.getPayload());
			g.writeBooleanField("stored", event.isStored());
			g.writeEndObject();
		}
		g.writeEndArray();
	}

	public void addEvent(Message message)
	{
		data.add(message);
		lastEventTime = DateTime.now();
		bytes.addAndGet(message.getPayload().length);

		if (firstEventTime == null)
		{
			firstEventTime = DateTime.now();
		}
	}

	public ChannelMetaData getChannelMetaData()
	{
		return channelMetaData;
	}

	public List<Message> getEvents()
	{
		return Collections.unmodifiableList(new ArrayList<>(data));
	}

	public long getBytes()
	{
		return bytes.get();
	}

	public DateTime getLastEventTime()
	{
		return lastEventTime;
	}

	public DateTime getFirstEventTime()
	{
		return firstEventTime;
	}

	public BatchMetaData getMetaData()
	{
		return new BatchMetaData(nearestPeriodCeiling, data.size(), bytes.get(), channelMetaData);
	}

	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Batch that = (Batch) o;

		if (channelMetaData != null ? !channelMetaData.equals(that.channelMetaData) : that.channelMetaData != null)
			return false;
		if (nearestPeriodCeiling != null ? !nearestPeriodCeiling.equals(that.nearestPeriodCeiling) : that.nearestPeriodCeiling != null)
			return false;

		return true;
	}

	public int hashCode()
	{
		int result = nearestPeriodCeiling != null ? nearestPeriodCeiling.hashCode() : 0;
		result = 31 * result + (channelMetaData != null ? channelMetaData.hashCode() : 0);
		return result;
	}
}
