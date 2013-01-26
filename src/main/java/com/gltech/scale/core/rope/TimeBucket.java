package com.gltech.scale.core.rope;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.gltech.scale.core.event.EventPayload;
import com.gltech.scale.core.storage.BucketMetaData;
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

public class TimeBucket
{
	private static final Logger logger = LoggerFactory.getLogger("com.lokiscale.rope.TimeBucket");
	private DateTime nearestPeriodCeiling;
	private DateTime firstEventTime;
	private DateTime lastEventTime = DateTime.now();
	private LinkedBlockingQueue<EventPayload> data = new LinkedBlockingQueue<>();
	private AtomicLong bytes = new AtomicLong(0);
	private BucketMetaData bucketMetaData;

	public TimeBucket(BucketMetaData bucketMetaData, DateTime nearestPeriodCeiling)
	{
		this.bucketMetaData = bucketMetaData;
		this.nearestPeriodCeiling = nearestPeriodCeiling;
	}

	public TimeBucket(InputStream in)
	{
		try
		{
			JsonFactory f = new MappingJsonFactory();
			JsonParser jp = f.createJsonParser(in);

			jp.nextToken();

			while (jp.nextToken() != JsonToken.END_OBJECT)
			{
				String fieldName = jp.getCurrentName();
				jp.nextToken();
				if ("data".equalsIgnoreCase(fieldName))
				{
					this.data = new LinkedBlockingQueue<>(jsonToEvents(jp));
				}
				else if ("lastEventTime".equals(fieldName))
				{
					this.lastEventTime = new DateTime(jp.getText());
				}
				else if ("firstEventTime".equals(fieldName))
				{
					this.firstEventTime = new DateTime(jp.getText());
				}
				else if ("nearestPeriodCeiling".equals(fieldName))
				{
					this.nearestPeriodCeiling = new DateTime(jp.getText());
				}
				else if ("bucketMetaData".equals(fieldName))
				{
					jp.nextToken();
					bucketMetaData = new BucketMetaData(jp.readValueAsTree().toString());
				}
				else if ("bytes".equals(fieldName))
				{
					this.bytes = new AtomicLong(jp.getLongValue());
				}
				else
				{
					throw new IllegalStateException("Unrecognized field '" + fieldName + "'!");
				}
			}
			jp.close();
		}
		catch (IOException e)
		{
			logger.warn("unable to parse json", e);
			throw new RuntimeException("unable to parse json", e);
		}
	}

	static private List<EventPayload> jsonToEvents(JsonParser jp) throws IOException
	{
		List<EventPayload> events = new ArrayList<>();

		while (jp.nextToken() != JsonToken.END_ARRAY)
		{
			events.add(EventPayload.jsonToEvent(jp));
		}

		return events;
	}

	static public List<EventPayload> jsonToEvents(InputStream in)
	{
		List<EventPayload> events;

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
			g.writeNumber(bucketMetaData.toJson().toString());
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

	static public void eventsToJson(List<EventPayload> events, OutputStream outputStream) throws IOException
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

	static private void eventsToJson(JsonGenerator g, List<EventPayload> events) throws IOException
	{
		g.writeStartArray();
		for (EventPayload event : events)
		{
			g.writeStartObject();
			g.writeStringField("customer", event.getCustomer());
			g.writeStringField("bucket", event.getBucket());
			g.writeStringField("received_at", event.getReceived_at().toString());
			g.writeStringField("uuid", event.getUuid());
			g.writeBinaryField("payload", event.getPayload());
			g.writeBooleanField("stored", event.isStored());
			g.writeEndObject();
		}
		g.writeEndArray();
	}

	public void addEvent(EventPayload eventPayload)
	{
		data.add(eventPayload);
		lastEventTime = DateTime.now();
		bytes.addAndGet(eventPayload.getPayload().length);

		if (firstEventTime == null)
		{
			firstEventTime = DateTime.now();
		}
	}

	public BucketMetaData getBucketMetaData()
	{
		return bucketMetaData;
	}

	public List<EventPayload> getEvents()
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

	public TimeBucketMetaData getMetaData()
	{
		return new TimeBucketMetaData(nearestPeriodCeiling, data.size(), bytes.get(), bucketMetaData);
	}

	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		TimeBucket that = (TimeBucket) o;

		if (bucketMetaData != null ? !bucketMetaData.equals(that.bucketMetaData) : that.bucketMetaData != null)
			return false;
		if (nearestPeriodCeiling != null ? !nearestPeriodCeiling.equals(that.nearestPeriodCeiling) : that.nearestPeriodCeiling != null)
			return false;

		return true;
	}

	public int hashCode()
	{
		int result = nearestPeriodCeiling != null ? nearestPeriodCeiling.hashCode() : 0;
		result = 31 * result + (bucketMetaData != null ? bucketMetaData.hashCode() : 0);
		return result;
	}
}
